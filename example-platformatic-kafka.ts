import { Producer, stringSerializers } from "@platformatic/kafka";

import expressWinston, { type LoggerOptions } from "express-winston";
import type { TransformableInfo } from "logform";
import type TransportStream from "winston-transport";
import Transport, { type TransportStreamOptions } from "winston-transport";
import type {
	Request,
	RequestHandler,
	Response,
} from "express";

// ***USELESS FUNC NOT REALLY NECESSARY TO DEBUG THE 'ISSUE'***
declare global {
	namespace Express {
		interface Request {
			_ip?: string;
			customRoute?: string;
			tenant?: Record<string, unknown>;
			imp_tenant?: Record<string, unknown>;
			auth?: {
				id?: string;
				email?: string;
				name?: string;
				type?: string;
				[key: string]: unknown;
			};
		}
		interface Response {
			responseTime?: number;
			body?: unknown;
		}
	}
}

interface ExpressWinstonLoggerParams {
	transports: TransportStream | KafkaTransport[];
	whitelistEnpoints?: string[];
	blacklistEnpoints?: string[];
	ignore_all_routes?: boolean;
}

export class KafkaTransport extends Transport {
	private kafkaCallback: (info: TransformableInfo) => Promise<void>;

	constructor(
		opts: TransportStreamOptions,
		kafkaCallback: (info: TransformableInfo) => Promise<void>,
	) {
		super(opts);
		this.kafkaCallback = kafkaCallback;
	}

	override log(info: TransformableInfo, callback: () => void): void {
		setImmediate(async () => {
			await this.kafkaCallback(info);
			this.emit("logged", info);
		});
		callback();
	}
}

export const expressWinstonLogger = ({
	transports = [],
	whitelistEnpoints = [],
	blacklistEnpoints = [],
	ignore_all_routes = false,
}: ExpressWinstonLoggerParams): RequestHandler => {
	const transportArray = Array.isArray(transports) ? transports : [transports];

	expressWinston.responseWhitelist.push("body");
	expressWinston.responseWhitelist.push("locals._action");
	expressWinston.requestWhitelist.push("params");

	const loggerOptions: LoggerOptions = {
		transports: transportArray,
		metaField: null,
		msg: "{{req.method}} {{req.url}} {{res.statusCode}}",
		ignoreRoute: (req: Request, _res: Response): boolean => {
			const sanitizedPath = req.originalUrl.replace(
				/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,
				":id",
			);
			const onlyPath = sanitizedPath.split("?")[0];

			if (
				(ignore_all_routes && !whitelistEnpoints.includes(req.originalUrl)) ||
				blacklistEnpoints.includes(req.originalUrl) ||
				blacklistEnpoints.includes(onlyPath)
			) {
				return true;
			}
			if (req.method === "OPTIONS" || req.method === "GET") {
				return true;
			}
			return false;
		},
		dynamicMeta: (req: Request, res: Response): Record<string, unknown> => {
			const meta: {
				update: { before: unknown; after: unknown };
				timestamp: Date;
				tokenPayload: Record<string, unknown>;
				remoteIp?: string;
				route?: string;
				requestLatency?: {
					seconds: number;
					nanos: number;
				};
			} = {
				update: {
					before: null,
					after: null,
				},
				tokenPayload: {},
				timestamp: new Date(),
			};

			const ip = req.ip ?? "";
			meta.remoteIp = req._ip || (ip.includes(":") ? ip.split(":").pop() : ip);
			meta.route = req.customRoute || req.route?.path;

			meta.tokenPayload = {
				tenant: req.tenant,
				imp_tenant: req.imp_tenant,
				auth: req.auth,
				user: {
					id: req.auth?.id,
					email: req.auth?.email,
					name: req.auth?.name,
				},
			};

			const action = (
				res.locals as {
					_action?: {
						updateBefore?: unknown;
						updateAfter?: unknown;
					};
				}
			)._action;

			if (action?.updateBefore !== undefined) {
				meta.update.before = action.updateBefore;
			}

			if (action?.updateAfter !== undefined || res.body !== undefined) {
				meta.update.after = action?.updateAfter ?? res.body ?? null;
			}

			if (res.responseTime !== undefined) {
				meta.requestLatency = {
					seconds: Math.floor(res.responseTime / 1000),
					nanos: (res.responseTime % 1000) * 1_000_000,
				};
			}

			return meta;
		},
		skip: (_req: Request, res: Response): boolean =>
			res.statusCode >= 400 && res.statusCode < 500,
	};

	return expressWinston.logger(loggerOptions);
};

// ***FINISH USELESS FUNC NOT REALLY NECESSARY TO DEBUG THE 'ISSUE'***

// Create a producer with string serialisers
const producer = new Producer({
	clientId: "openstack-cloud-v3-api-gateway",
	bootstrapBrokers: ['localhost:9092'],
	serializers: stringSerializers,
	connectTimeout: 10000,
	retries: 3,
	retryDelay: 1000,
});


const kafkaCallback = async (info: unknown): Promise<void> => {
	if (!producer) {
		console.warn("Kafka producer is not initialized.");
		return;
	}
    // *** Problematic SEND ***
    // *** WITHOUT option 'KEY' SET Everything crashes *** => But it all messages into one partition
	try {
		await producer.send({
			messages: [
				{
					topic: "logs.audit",
					key: "openstack-cloud:audit-log",
					value: JSON.stringify(info),
				},
			],
			// acks: 1 //To set to -1 in production environment
		});
	} catch (_error) {
		// Gracefully handle Kafka unavailability - don't crash the application
		console.warn("Kafka unavailable, skipping log message");
	}
};

// Pass the callback function reference directly. KafkaTransport will call it later.
export const kafkaTransport = new KafkaTransport({}, kafkaCallback);
