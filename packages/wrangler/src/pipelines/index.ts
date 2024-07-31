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
	PipelineConfig,
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
					});
			},
			async (args) => {
				await printWranglerBanner();

				const config = readConfig(args.config, args);
				const bucket = args.r2
				const name = args.pipeline;

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

				const pipelineConfig: PipelineConfig = {
					name,
					input: {
						JSON: {}
					},
					destination: {
						R2_BUCKET: {
							bucket,
							credentials: {
								endpoint,
								access_key_id,
								secret_access_key,
							},
							output_format: {
								JSON: {}
							}
						}
					}
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