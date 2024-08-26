import { createHash } from "node:crypto";
import { fetchResult } from "../cfetch";
import { R2BucketInfo } from "../r2/helpers";

// ensure this is in sync with:
//   https://bitbucket.cfdata.org/projects/PIPE/repos/superpipe/browse/src/coordinator/types.ts#6

export type TransformConfig = {
	script: string,
	entrypoint: string
}
export type PipelineUserConfig = {
  name: string
  metadata: {[x: string]: string}
  source: {
    type: string
    format: string
		schema?: string
  }[],
  transforms: TransformConfig[]
  destination: {
    type: string
    format: string
    compression: {
      type: string
    },
		batch: {
			max_duration_s?: number
			max_mb?: number
			max_rows?: number
		},
    path: {
      bucket: string
			filepath?: string
			filename?: string
    },
    credentials: {
      endpoint: string
      secret_access_key: string
      access_key_id: string
    },
  },
}

// Pipeline from v4 API
export type Pipeline = PipelineUserConfig & {
	id: string
	currentVersion: number
	endpoint: string
}

// abbreviated Pipeline from Pipeline list call
export type PipelineEntry = {
	id: string
	name: string
	endpoint: string
}

// Payload for Service Tokens
export type ServiceToken = {
	id: string
	name: string
	value: string
}

// standard headers for update calls to v4 API
const API_HEADERS = {
	'Content-Type': 'application/json',
}

export function sha256(s: string): string {
	return createHash('sha256').update(s).digest('hex')
}

// Generate a Service Token to write to R2 for a pipeline
export async function generateR2ServiceToken(label: string, accountId: string, bucket: string): Promise<ServiceToken> {
	let res = await fetchResult<any>(`/user/tokens/permission_groups`, {
		method: 'GET',
	})
	const perm = res.find((g: any) => g.name == 'Workers R2 Storage Bucket Item Write')
	if (!perm) {
		throw new Error("Missing R2 Permissions")
	}

	// generate specific bucket write token for pipeline
	const body = JSON.stringify({
		policies: [{
			effect: "allow",
			permission_groups: [{ "id": perm.id }],
			resources: {
				[`com.cloudflare.edge.r2.bucket.${accountId}_default_${bucket}`]: "*",
			},
		}],
		name: label,
	})

	return await fetchResult<ServiceToken>(`/user/tokens`, {
		method: "POST",
		headers: API_HEADERS,
		body,
	});
}

// Get R2 bucket information from v4 API
export async function getR2Bucket(accountId: string, name: string): Promise<R2BucketInfo> {
	return await fetchResult<R2BucketInfo>(`/accounts/${accountId}/r2/buckets/${name}`)
}

// v4 API to Create new Pipeline
export async function createPipeline(accountId: string, config: PipelineUserConfig): Promise<Pipeline> {
	return await fetchResult<Pipeline>(`/accounts/${accountId}/pipelines`, {
		method: "POST",
		headers: API_HEADERS,
		body: JSON.stringify(config),
	});
}

// v4 API to Get Pipeline Details
export async function getPipeline(accountId: string, id: string): Promise<Pipeline> {
	return await fetchResult<Pipeline>(`/accounts/${accountId}/pipelines/${id}`, {
		method: "GET",
	});
}

// v4 API to Update Pipeline Configuration
export async function updatePipeline(accountId: string, id: string, config: PipelineUserConfig): Promise<Pipeline> {
	return await fetchResult<Pipeline>(`/accounts/${accountId}/pipelines/${id}`, {
		method: "PUT",
		headers: API_HEADERS,
		body: JSON.stringify({
			name: 'foo',
			config,
		}),
	});
}

// v4 API to List Available Pipelines
export async function listPipelines(accountId: string): Promise<PipelineEntry[]> {
	return await fetchResult<PipelineEntry[]>(`/accounts/${accountId}/pipelines`, {
		method: "GET",
	});
}

// v4 API to Delete Pipeline
export async function deletePipeline(accountId: string, id: string): Promise<void> {
	return await fetchResult<void>(`/accounts/${accountId}/pipelines/${id}`, {
		method: "DELETE",
		headers: API_HEADERS,
	});
}

