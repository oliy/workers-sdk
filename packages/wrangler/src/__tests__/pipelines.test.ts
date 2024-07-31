import { http, HttpResponse } from "msw";
import { __testSkipDelays } from "../pipelines";
import { endEventLoop } from "./helpers/end-event-loop";
import { mockAccountId, mockApiToken } from "./helpers/mock-account-id";
import { mockConsoleMethods } from "./helpers/mock-console";
import { msw } from "./helpers/msw";
import { runInTempDir } from "./helpers/run-in-tmp";
import { runWrangler } from "./helpers/run-wrangler";
import type { Pipeline, PipelineEntry } from "../pipelines/client";

describe("pipelines", () => {
	const std = mockConsoleMethods();
	mockAccountId();
	mockApiToken();
	runInTempDir();

	const samplePipeline = {
		currentVersion: 1,
		id: "0001",
		name: "my-pipeline",
		metadata: {},
		source: [
			{
				type: "binding",
				format: "json",
			},
		],
		transforms: [],
		destination: {
			type: "json",
			batch: {},
			compression: {
				type: "none",
			},
			format: "json",
			path: {
				bucket: "bucket",
			},
		},
		endpoint: "https://0001.pipelines.cloudflarestorage.com",
	};

	function mockCreateR2Token(bucket: string) {
		const requests = { count: 0 };
		msw.use(
			http.get(
				"*/accounts/:accountId/r2/buckets/:bucket",
				async ({ params }) => {
					expect(params.accountId).toEqual("some-account-id");
					expect(params.bucket).toEqual(bucket);
					requests.count++;
					return HttpResponse.json(
						{
							success: true,
							errors: [],
							messages: [],
							result: null,
						},
						{ status: 200 }
					);
				},
				{ once: true }
			),
			http.get(
				"*/user/tokens/permission_groups",
				async () => {
					requests.count++;
					return HttpResponse.json(
						{
							success: true,
							errors: [],
							messages: [],
							result: [
								{
									id: "2efd5506f9c8494dacb1fa10a3e7d5b6",
									name: "Workers R2 Storage Bucket Item Write",
									description:
										"Grants write access to Cloudflare R2 Bucket Scoped Storage",
									scopes: ["com.cloudflare.edge.r2.bucket"],
								},
							],
						},
						{ status: 200 }
					);
				},
				{ once: true }
			),
			http.post(
				"*/user/tokens",
				async () => {
					requests.count++;
					return HttpResponse.json(
						{
							success: true,
							errors: [],
							messages: [],
							result: {
								id: "service-token-id",
								name: "my-service-token",
								value: "my-secret-value",
							},
						},
						{ status: 200 }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}

	function mockCreeatR2TokenFailure(bucket: string) {
		const requests = { count: 0 };
		msw.use(
			http.get(
				"*/accounts/:accountId/r2/buckets/:bucket",
				async ({ params }) => {
					expect(params.accountId).toEqual("some-account-id");
					expect(params.bucket).toEqual(bucket);
					requests.count++;
					return HttpResponse.json(
						{
							success: false,
							errors: [
								{
									code: 10006,
									message: "The specified bucket does not exist.",
								},
							],
							messages: [],
							result: null,
						},
						{ status: 404 }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}

	function mockCreateRequest(
		name: string,
		status: number = 200,
		error?: object
	) {
		const requests = { count: 0 };
		msw.use(
			http.post(
				"*/accounts/:accountId/pipelines",
				async ({ request, params }) => {
					expect(params.accountId).toEqual("some-account-id");
					const config = (await request.json()) as Pipeline;
					expect(config.name).toEqual(name);
					requests.count++;
					const pipeline: Pipeline = {
						...config,
						id: "0001",
						name: name,
						endpoint: "foo",
					};
					return HttpResponse.json(
						{
							success: !error,
							errors: error ? [error] : [],
							messages: [],
							result: pipeline,
						},
						{ status: status }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}

	function mockListRequest(entries: PipelineEntry[]) {
		const requests = { count: 0 };
		msw.use(
			http.get(
				"*/accounts/:accountId/pipelines",
				async ({ params }) => {
					requests.count++;
					expect(params.accountId).toEqual("some-account-id");

					return HttpResponse.json(
						{
							success: true,
							errors: [],
							messages: [],
							result: entries,
						},
						{ status: 200 }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}

	function mockShowRequest(
		name: string,
		pipeline: Pipeline | null,
		status: number = 200,
		error?: object
	) {
		const requests = { count: 0 };
		msw.use(
			http.get(
				"*/accounts/:accountId/pipelines/:name",
				async ({ params }) => {
					requests.count++;
					expect(params.accountId).toEqual("some-account-id");
					expect(params.name).toEqual(name);

					return HttpResponse.json(
						{
							success: !error,
							errors: error ? [error] : [],
							messages: [],
							result: pipeline,
						},
						{ status }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}

	function mockUpdateRequest(
		name: string,
		pipeline: Pipeline | null,
		status: number = 200,
		error?: object
	) {
		const requests: { count: number; body: Pipeline | null } = {
			count: 0,
			body: null,
		};
		msw.use(
			http.put(
				"*/accounts/:accountId/pipelines/:name",
				async ({ params, request }) => {
					requests.count++;
					requests.body = (await request.json()) as Pipeline;
					expect(params.accountId).toEqual("some-account-id");
					expect(params.name).toEqual(name);

					// update strips creds, so enforce this
					if (pipeline?.destination) {
						pipeline.destination.credentials = undefined;
					}

					return HttpResponse.json(
						{
							success: !error,
							errors: error ? [error] : [],
							messages: [],
							result: pipeline,
						},
						{ status }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}

	function mockDeleteRequest(
		name: string,
		status: number = 200,
		error?: object
	) {
		const requests = { count: 0 };
		msw.use(
			http.delete(
				"*/accounts/:accountId/pipelines/:name",
				async ({ params }) => {
					requests.count++;
					expect(params.accountId).toEqual("some-account-id");
					expect(params.name).toEqual(name);

					return HttpResponse.json(
						{
							success: !error,
							errors: error ? [error] : [],
							messages: [],
							result: null,
						},
						{ status }
					);
				},
				{ once: true }
			)
		);
		return requests;
	}
	beforeAll(() => {
		__testSkipDelays();
	});

	it("shows usage details", async () => {
		await runWrangler("pipelines");
		await endEventLoop();

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
          "wrangler pipelines

          🚰 Manage Worker Pipelines


          COMMANDS
            wrangler pipelines create <pipeline>  Create a new pipeline
            wrangler pipelines list               List current pipelines
            wrangler pipelines show <pipeline>    Show a pipeline configuration
            wrangler pipelines update <pipeline>  Update a pipeline
            wrangler pipelines delete <pipeline>  Delete a pipeline

          GLOBAL FLAGS
            -j, --experimental-json-config  Experimental: support wrangler.json  [boolean]
            -c, --config                    Path to .toml configuration file  [string]
            -e, --env                       Environment to use for operations and .env files  [string]
            -h, --help                      Show help  [boolean]
            -v, --version                   Show version number  [boolean]"
        `);
	});

	it("create - should create a pipeline", async () => {
		const tokenReq = mockCreateR2Token("test-bucket");
		const requests = mockCreateRequest("my-pipeline");
		await runWrangler("pipelines create my-pipeline --r2 test-bucket");

		expect(tokenReq.count).toEqual(3);
		expect(requests.count).toEqual(1);
	});

	it("create - should create a pipeline with explicit credentials", async () => {
		const requests = mockCreateRequest("my-pipeline");
		await runWrangler(
			"pipelines create my-pipeline --r2 test-bucket --access-key-id my-key --secret-access-key my-secret"
		);
		expect(requests.count).toEqual(1);
	});

	it("create - should fail a missing bucket", async () => {
		const requests = mockCreeatR2TokenFailure("bad-bucket");
		await expect(
			runWrangler("pipelines create bad-pipeline --r2 bad-bucket")
		).rejects.toThrowError();

		await endEventLoop();

		expect(std.err).toMatchInlineSnapshot(`
			"[31mX [41;31m[[41;97mERROR[41;31m][0m [1mThe R2 bucket [bad-bucket] doesn't exist[0m

			"
		`);
		expect(std.out).toMatchInlineSnapshot(`""`);
		expect(requests.count).toEqual(1);
	});

	it("list - should list pipelines", async () => {
		const requests = mockListRequest([
			{
				name: "foo",
				id: "0001",
				endpoint: "https://0001.pipelines.cloudflarestorage.com",
			},
		]);
		await runWrangler("pipelines list");

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
			"┌──────┬──────┬──────────────────────────────────────────────┐
			│ name │ id   │ endpoint                                     │
			├──────┼──────┼──────────────────────────────────────────────┤
			│ foo  │ 0001 │ https://0001.pipelines.cloudflarestorage.com │
			└──────┴──────┴──────────────────────────────────────────────┘"
		`);
		expect(requests.count).toEqual(1);
	});

	it("show - should show pipeline", async () => {
		const requests = mockShowRequest("foo", samplePipeline);
		await runWrangler("pipelines show foo");

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
			"Retrieving config for pipeline \\"foo\\".
			{
			  \\"currentVersion\\": 1,
			  \\"id\\": \\"0001\\",
			  \\"name\\": \\"my-pipeline\\",
			  \\"metadata\\": {},
			  \\"source\\": [
			    {
			      \\"type\\": \\"binding\\",
			      \\"format\\": \\"json\\"
			    }
			  ],
			  \\"transforms\\": [],
			  \\"destination\\": {
			    \\"type\\": \\"json\\",
			    \\"batch\\": {},
			    \\"compression\\": {
			      \\"type\\": \\"none\\"
			    },
			    \\"format\\": \\"json\\",
			    \\"path\\": {
			      \\"bucket\\": \\"bucket\\"
			    }
			  },
			  \\"endpoint\\": \\"https://0001.pipelines.cloudflarestorage.com\\"
			}"
		`);
		expect(requests.count).toEqual(1);
	});

	it("show - should fail on missing pipeline", async () => {
		const requests = mockShowRequest("bad-pipeline", null, 404, {
			code: 1000,
			message: "Pipeline does not exist",
		});
		await expect(
			runWrangler("pipelines show bad-pipeline")
		).rejects.toThrowError();

		await endEventLoop();

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
			"Retrieving config for pipeline \\"bad-pipeline\\".

			[31mX [41;31m[[41;97mERROR[41;31m][0m [1mA request to the Cloudflare API (/accounts/some-account-id/pipelines/bad-pipeline) failed.[0m

			  Pipeline does not exist [code: 1000]

			  If you think this is a bug, please open an issue at:
			  [4mhttps://github.com/cloudflare/workers-sdk/issues/new/choose[0m

			"
		`);
		expect(requests.count).toEqual(1);
	});

	it("update - should update a pipeline", async () => {
		const pipeline: Pipeline = samplePipeline;
		mockShowRequest(pipeline.name, pipeline);

		const update = JSON.parse(JSON.stringify(pipeline));
		update.destination.compression.type = "gzip";
		const updateReq = mockUpdateRequest(update.name, update);

		await runWrangler("pipelines update my-pipeline --compression gzip");
		expect(updateReq.count).toEqual(1);
	});

	it("update - should update a pipeline with new bucket", async () => {
		const pipeline: Pipeline = samplePipeline;
		const tokenReq = mockCreateR2Token("new-bucket");
		mockShowRequest(pipeline.name, pipeline);

		const update = JSON.parse(JSON.stringify(pipeline));
		update.destination.path.bucket = "new_bucket";
		update.destination.credentials = {
			endpoint: "https://some-account-id.r2.cloudflarestorage.com",
			access_key_id: "service-token-id",
			secret_access_key:
				"be22cbae9c1585c7b61a92fdb75afd10babd535fb9b317f90ac9a9ca896d02d7",
		};
		const updateReq = mockUpdateRequest(update.name, update);

		await runWrangler("pipelines update my-pipeline --r2 new-bucket");

		expect(tokenReq.count).toEqual(3);
		expect(updateReq.count).toEqual(1);
	});

	it("update - should update a pipeline with new credential", async () => {
		const pipeline: Pipeline = samplePipeline;
		mockShowRequest(pipeline.name, pipeline);

		const update = JSON.parse(JSON.stringify(pipeline));
		update.destination.path.bucket = "new-bucket";
		update.destination.credentials = {
			endpoint: "https://some-account-id.r2.cloudflarestorage.com",
			access_key_id: "new-key",
			secret_access_key: "new-secret",
		};
		const updateReq = mockUpdateRequest(update.name, update);

		await runWrangler(
			"pipelines update my-pipeline --r2 new-bucket --access-key-id new-key --secret-access-key new-secret"
		);

		expect(updateReq.count).toEqual(1);
	});

	it("update - should fail a missing pipeline", async () => {
		const requests = mockShowRequest("bad-pipeline", null, 404, {
			code: 1000,
			message: "Pipeline does not exist",
		});
		await expect(
			runWrangler(
				"pipelines update bad-pipeline --r2 new-bucket --access-key-id new-key --secret-access-key new-secret"
			)
		).rejects.toThrowError();

		await endEventLoop();

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
			"
			[31mX [41;31m[[41;97mERROR[41;31m][0m [1mA request to the Cloudflare API (/accounts/some-account-id/pipelines/bad-pipeline) failed.[0m

			  Pipeline does not exist [code: 1000]

			  If you think this is a bug, please open an issue at:
			  [4mhttps://github.com/cloudflare/workers-sdk/issues/new/choose[0m

			"
		`);
		expect(requests.count).toEqual(1);
	});

	it("delete - should delete pipeline", async () => {
		const requests = mockDeleteRequest("foo");
		await runWrangler("pipelines delete foo");

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
			"Deleting pipeline foo.
			Deleted pipeline foo."
		`);
		expect(requests.count).toEqual(1);
	});

	it("delete - should fail a missing pipeline", async () => {
		const requests = mockDeleteRequest("bad-pipeline", 404, {
			code: 1000,
			message: "Pipeline does not exist",
		});
		await expect(
			runWrangler("pipelines delete bad-pipeline")
		).rejects.toThrowError();

		await endEventLoop();

		expect(std.err).toMatchInlineSnapshot(`""`);
		expect(std.out).toMatchInlineSnapshot(`
			"Deleting pipeline bad-pipeline.

			[31mX [41;31m[[41;97mERROR[41;31m][0m [1mA request to the Cloudflare API (/accounts/some-account-id/pipelines/bad-pipeline) failed.[0m

			  Pipeline does not exist [code: 1000]

			  If you think this is a bug, please open an issue at:
			  [4mhttps://github.com/cloudflare/workers-sdk/issues/new/choose[0m

			"
		`);
		expect(requests.count).toEqual(1);
	});
});
