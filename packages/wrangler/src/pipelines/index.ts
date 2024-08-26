import { readConfig } from "../config";
import { sleep } from "../deploy/deploy";
import { FatalError } from "../errors";
import {
	printWranglerBanner
} from "../index";
import { logger } from "../logger";
import * as metrics from "../metrics";
import { requireAuth } from "../user";
import type { CommonYargsArgv, SubHelp } from "../yargs-types";
import {
	createPipeline,
	deletePipeline,
	generateR2ServiceToken,
	getR2Bucket,
	listPipelines,
	PipelineUserConfig,
	sha256,
} from "./client";

const rePipelineId = /^[0-9a-fA-F]{32}$/

export function pipelines(yargs: CommonYargsArgv, subHelp: SubHelp) {
	return yargs
		.command(
			"create <pipeline>",
			"Create a new pipeline",
			(yargs) => {
				return yargs
					.positional("pipeline", {
						describe: "The name of the new pipeline",
						type: "string",
						demandOption: true,
					})
					.option("r2", {
						type: "string",
						describe: "Create a default aggregation pipeline to the named R2 bucket",
						demandOption: true
					})
					.option("batch-max-mb", {
						describe: "The maximum size of a batch before flush in megabytes",
						type: "string",
						demandOption: false,
					})
					.option("batch-max-rows", {
						describe: "The maximum size of a batch before flush in rows",
						type: "string",
						demandOption: false,
					})
					.option("batch-max-seconds", {
						describe: "The maximum duration of a batch before flush in seconds",
						type: "string",
						demandOption: false,
					})
					.option("transform", {
						describe: "The name of the script and entrypoint where the transformation is implemented in the format 'script.entrypoint'",
						type: "string",
						demandOption: false,
					})
					.option("compression", {
						describe: "Sets the compression format of output files",
						type: "string",
						choices: ['none', 'gzip', 'deflate'],
						demandOption: false,
					})
					.option("filepath", {
						describe: "The path to store the file in the bucket",
						type: "string",
						demandOption: false,
					})
					.option("filename", {
						describe: "The name of the file in the bucket. Must contain '${slug}'",
						type: "string",
						demandOption: false,
					})
					.option("authentication", {
						describe: "Enabling authentication means that data can only be sent to the pipeline via the binding",
						type: "boolean",
						demandOption: false,
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
					max_duration: args['batch-max-duration'],
					max_rows: args['batch-max-rows']
				}

				if (!config.name) {
					logger.warn(
						"No configured name present, using `worker` as a prefix for the title"
					);
				}
				if (!bucket) {
					throw new FatalError("Requires a r2 bucket")
				}
				const accountId = await requireAuth(config);

				if (!await getR2Bucket(accountId, bucket)) {
					logger.warn("Specified bucket doesn't exist:", bucket)
				}

				logger.log(`🌀 Authorizing R2 bucket "${bucket}"`);

				const serviceToken = await generateR2ServiceToken(`Service token for Pipeline ${name}`,
					accountId, bucket)

				const access_key_id = serviceToken.id
				const secret_access_key = sha256(serviceToken.value)
				const endpoint = `https://${accountId}.r2.cloudflarestorage.com`

				// wait for token to settle/propagate
				await sleep(3000)

				logger.log(`🌀 Creating pipeline named "${name}"`);

				const pipelineConfig: PipelineUserConfig = {
					name: name,
					metadata: {},
					source: {
						type: 'http',
						format: 'json',
					},
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

				if (args.authentication) {
					pipelineConfig.source.type = 'binding-only'
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

				const pipeline = await createPipeline(accountId, pipelineConfig);
				await metrics.sendMetricsEvent("create pipeline", {
					sendMetrics: config.send_metrics,
				});

				logger.log(`✅ Successfully created pipeline "${pipeline.name}" with ID ${pipeline.id}`);
				logger.log(`\nYou can now send data to your pipeline with:\n  curl "${pipeline.endpoint}/send" -d '[{ ...JSON_DATA... }]'`);
			}
		)
		.command(
			"list",
			"List current pipelines",
			(yargs) => yargs,
			async (args) => {
				const config = readConfig(args.config, args);

				const accountId = await requireAuth(config);

				// TODO: we should show bindings if they exist for given ids

				const entries = await listPipelines(accountId)
				await metrics.sendMetricsEvent("list pipelines", {
					sendMetrics: config.send_metrics,
				});

				logger.log("Pipelines: name (id) - endpoint")

				entries.forEach(({ id, name, endpoint }) => {
					logger.log(`  ${name} (${id}) - ${endpoint}`)
				})
			}
		)
		.command(
			"delete <pipeline-id>",
			"Delete a pipeline",
			(yargs) => {
				return yargs
					.positional("pipeline-id", {
						type: "string",
						describe: "The id of the pipeline to delete",
					})
			},
			async (args) => {
				await printWranglerBanner();
				const config = readConfig(args.config, args);
				const accountId = await requireAuth(config);
				const id = args.pipelineId!

				if (!id.match(/[0-9a-f]{32}/)) {
					throw new Error('must provide a valid pipeline id')
				}

				logger.log(`Deleting pipeline ${id}.`);
				await deletePipeline(accountId, id);
				logger.log(`Deleted pipeline ${id}.`);
				await metrics.sendMetricsEvent("delete pipeline", {
					sendMetrics: config.send_metrics,
				});
			}
		);
}