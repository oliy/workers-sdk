import yargs from "yargs";
import { readConfig } from "../config";
import { sleep } from "../deploy/deploy";
import { FatalError } from "../errors";
import {
	printWranglerBanner
} from "../index";
import { logger } from "../logger";
import * as metrics from "../metrics";
import { requireAuth } from "../user";
import type { CommonYargsArgv, CommonYargsOptions, SubHelp } from "../yargs-types";
import {
	createPipeline,
	deletePipeline,
	generateR2ServiceToken,
	getR2Bucket,
	listPipelines,
	updatePipeline,
	PipelineUserConfig,
	sha256,
	PartialExcept
} from "./client";

function addCreateAndUpdateOptions(yargs: yargs.Argv<CommonYargsOptions>) {
	return yargs
		.option("batch-max-mb", {
			describe: `
				The approximate maximum size of a batch before flush in megabytes
				Default: 10
			`,
			type: "number",
			demandOption: false,
		})
		.option("batch-max-rows", {
			describe: `
				The approximate maximum size of a batch before flush in rows
				Default: 10000
			`,
			type: "number",
			demandOption: false,
		})
		.option("batch-max-seconds", {
			describe: `
				The approximate maximum duration of a batch before flush in seconds
				Default: 15
			`,
			type: "number",
			demandOption: false,
		})
		.option("transform", {
			describe: `
				The worker and entrypoint of the PipelineTransform implementation in the format 'worker.entrypoint'
				Default: No transformation
			`,
			type: "string",
			demandOption: false,
		})
		.option("compression", {
			describe: `
				Sets the compression format of output files
				Default: none
			`,
			type: "string",
			choices: ['none', 'gzip', 'deflate'],
			demandOption: false,
		})
		.option("filepath", {
			describe: `
				The path to store files in the destination bucket
				Default: event_date=\${date}/hr=\${hr}
			`,
			type: "string",
			demandOption: false,
		})
		.option("filename", {
			describe: `
				The name of the file in the bucket. Must contain '\${slug}'. File extension is optional
				Default: \${slug}-\${hr}.json
			`,
			type: "string",
			demandOption: false,
		})
		.option("authentication", {
			describe: `
				Enabling authentication means that data can only be sent to the pipeline via the binding
				Default: false
			`,
			type: "boolean",
			demandOption: false,
		})
}

export function pipelines(yargs: CommonYargsArgv, subHelp: SubHelp) {
	return yargs
		.command(
			"create <pipeline>",
			"Create a new pipeline",
			(yargs) => {
				return addCreateAndUpdateOptions(yargs)
					.positional("pipeline", {
						describe: "The name of the new pipeline",
						type: "string",
						demandOption: true,
					})
					.option("r2", {
						type: "string",
						describe: "Destination R2 bucket name",
						demandOption: true
					})
			},
			async (args) => {
				await printWranglerBanner();

				const config = readConfig(args.config, args);
				const bucket = args.r2
				const name = args.pipeline;
				const compression = args.compression === undefined ? 'none' : args.compression

				const batch = {
					max_mb: args['batch-max-mb'],
					max_duration: args['batch-max-seconds'],
					max_rows: args['batch-max-rows']
				}

				if (!bucket) {
					throw new FatalError("Requires a r2 bucket")
				}

				const accountId = await requireAuth(config);
				if (!await getR2Bucket(accountId, bucket)) {
					logger.warn("Specified bucket doesn't exist:", bucket)
				}

				logger.log(`🌀 Authorizing R2 bucket "${bucket}"`);

				const serviceToken = await generateR2ServiceToken(`Service token for Pipeline ${name}`, accountId, bucket)
				const access_key_id = serviceToken.id
				const secret_access_key = sha256(serviceToken.value)
				const endpoint = `https://${accountId}.r2.cloudflarestorage.com`

				// wait for token to settle/propagate
				await sleep(3000)

				const pipelineConfig: PipelineUserConfig = {
					name: name,
					metadata: {},
					source: [
						{
							type: 'http',
							format: 'json',
						},
						{
							type: 'binding',
							format: 'json',
						},
					],
					transforms: [],
					destination: {
						type: 'r2',
						format: 'json',
						compression: {
							type: compression,
						},
						batch: batch,
						path: {
							bucket: bucket,
						},
						credentials: {
							endpoint: endpoint,
							secret_access_key: secret_access_key,
							access_key_id: access_key_id
						},
					},
				}

				if (args.authentication !== undefined && args.authentication === true) {
					pipelineConfig.source = [{
						type: 'binding',
						format: 'json'
					}]
				}

				if (args.transform !== undefined) {
					const split = args.transform.split('.')
					if (split.length === 2) {
						pipelineConfig.transforms.push({
							script: split[0],
							entrypoint: split[1]
						})
					} else if (split.length === 1) {
						pipelineConfig.transforms.push({
							script: split[0],
							entrypoint: 'Transform'
						})
					} else {
						throw new Error('invalid transform: required syntax script.entrypoint')
					}
				}

				if (args.filepath) {
					pipelineConfig.destination.path.filepath = args.filepath
				}
				if (args.filename) {
					pipelineConfig.destination.path.filename = args.filename
				}

				logger.log(`🌀 Creating pipeline named "${name}"`);
				const pipeline = await createPipeline(accountId, pipelineConfig);
				await metrics.sendMetricsEvent("create pipeline", {
					sendMetrics: config.send_metrics,
				});

				logger.log(`✅ Successfully created pipeline "${pipeline.name}" with ID ${pipeline.id}\n`);
				logger.log(`
					You can now send data to your pipeline!\n
					Simple Example: curl "${pipeline.endpoint}/send" -d '[{"foo": "bar"}]'
					Load Example:
				`);
			}
		)
		.command(
			"list",
			"List current pipelines",
			(yargs) => yargs,
			async (args) => {
				const config = readConfig(args.config, args);
				const accountId = await requireAuth(config);

				// TODO: we should show bindings & transforms if they exist for given ids
				const pipelines = await listPipelines(accountId)
				await metrics.sendMetricsEvent("list pipelines", {
					sendMetrics: config.send_metrics,
				});

				logger.table(
					pipelines.map((pipeline) => ({
						name: pipeline.name,
						id: pipeline.id,
						endpoint: pipeline.endpoint
					}))
				);
			}
		)
		.command(
			"delete <pipeline-name>",
			"Delete a pipeline",
			(yargs) => {
				return yargs
					.positional("pipeline", {
						type: "string",
						describe: "The name of the pipeline to delete",
						demandOption: true
					})
			},
			async (args) => {
				await printWranglerBanner();
				const config = readConfig(args.config, args);
				const accountId = await requireAuth(config);
				const name = args.pipeline

				if (!name.match(/^[a-zA-Z0-9-]+$/)) {
					throw new Error('must provide a valid pipeline name')
				}

				logger.log(`Deleting pipeline ${name}.`);
				await deletePipeline(accountId, name);
				logger.log(`Deleted pipeline ${name}.`);
				await metrics.sendMetricsEvent("delete pipeline", {
					sendMetrics: config.send_metrics,
				});
			}
		)
		.command(
			"update <pipeline-name>",
			"Update a pipeline",
			(yargs) => {
				return addCreateAndUpdateOptions(yargs)
					.positional("pipeline", {
						describe: "The name of the pipeline to update",
						type: "string",
						demandOption: true,
					})
					.option("r2", {
						type: "string",
						describe: "Destination R2 bucket name",
						demandOption: false
					})
			},
			async (args) => {
				await printWranglerBanner();

				const name = args.pipeline
				// only the fields set will be updated - other fields will use the existing config
				const pipelineConfig: PartialExcept<PipelineUserConfig, "name"> = {
					name: name,
					metadata: {},
				}

				const config = readConfig(args.config, args);
				const accountId = await requireAuth(config);
				const bucket = args.r2
				if (args.compression !== undefined) {
					if (pipelineConfig.destination === undefined) {
						pipelineConfig.destination = {
							compression: {
								type: args.compression
							}
						}
					} else {
						pipelineConfig.destination.compression = {
							type: args.compression
						}
					}
				}

				const batch = {
					max_mb: args['batch-max-mb'],
					max_duration: args['batch-max-seconds'],
					max_rows: args['batch-max-rows']
				}

				if (pipelineConfig.destination === undefined) {
					pipelineConfig.destination = {
						batch: batch
					}
				} else {
					pipelineConfig.destination.batch = batch
				}


				if (bucket) {
					if (!await getR2Bucket(accountId, bucket)) {
						logger.warn("Specified bucket doesn't exist:", bucket)
					}

					logger.log(`🌀 Authorizing R2 bucket "${bucket}"`);

					const serviceToken = await generateR2ServiceToken(`Service token for Pipeline ${name}`, accountId, bucket)
					const access_key_id = serviceToken.id
					const secret_access_key = sha256(serviceToken.value)
					const endpoint = `https://${accountId}.r2.cloudflarestorage.com`

					if (pipelineConfig.destination === undefined) {
						pipelineConfig.destination = {
							path: {
								bucket: bucket,
							},
							credentials: {
								endpoint: endpoint,
								secret_access_key: secret_access_key,
								access_key_id: access_key_id
							},
						}
					} else {
						pipelineConfig.destination.path = {
							bucket: bucket,
						}
						pipelineConfig.destination.credentials = {
							endpoint: endpoint,
							secret_access_key: secret_access_key,
							access_key_id: access_key_id
						}
					}

					// wait for token to settle/propagate
					await sleep(3000)
				}

				if (args.authentication !== undefined && args.authentication === true) {
					pipelineConfig.source = [{
						type: 'binding',
						format: 'json'
					}]
				}

				if (args.transform !== undefined) {
					pipelineConfig.transforms = []
					const split = args.transform.split('.')
					if (split.length === 2) {
						pipelineConfig.transforms.push({
							script: split[0],
							entrypoint: split[1]
						})
					} else if (split.length === 1) {
						pipelineConfig.transforms.push({
							script: split[0],
							entrypoint: 'Transform'
						})
					} else {
						throw new Error('invalid transform: required syntax script.entrypoint')
					}
				}


				if (args.filepath || args.filename) {
					if (pipelineConfig.destination === undefined) {
						pipelineConfig.destination = {
							path: {
								filepath: args.filepath,
								filename: args.filename
							}
						}
					} else if (pipelineConfig.destination.path === undefined) {
						pipelineConfig.destination.path = {
							filepath: args.filepath,
							filename: args.filename
						}
					}
				}

				logger.log(`🌀 Updating pipeline named "${name}"`);
				const pipeline = await updatePipeline(accountId, name, pipelineConfig);
				await metrics.sendMetricsEvent("update pipeline", {
					sendMetrics: config.send_metrics,
				});

				logger.log(`✅ Successfully updated pipeline "${pipeline.name}" with ID ${pipeline.id}\n`);
			}
		);
}