declare namespace Bun {
    function sleep(ms: number): Promise<void>
    function randomUUIDv7(): string
    function spawn(
        cmd: string[],
        options?: {
            stdin?: Blob | 'pipe'
            stdout?: 'pipe' | 'inherit'
            stderr?: 'pipe' | 'inherit'
            cwd?: string
        }
    ): {
        stdout?: ReadableStream<Uint8Array>
        stderr?: ReadableStream<Uint8Array>
        exited: Promise<number>
    }

    class Glob {
        constructor(pattern: string)
        match(input: string): boolean
    }

    namespace JSONL {
        function parseChunk(input: Uint8Array): { values: unknown[]; read: number }
    }

    interface S3ListObjectsOptions {
        delimiter?: string
        prefix?: string
        maxKeys?: number
        continuationToken?: string
    }

    interface S3ListObjectsResponse {
        commonPrefixes?: Array<{ prefix?: string }>
        contents?: Array<{ key?: string }>
        isTruncated?: boolean
        nextContinuationToken?: string
    }
}

declare module 'bun' {
    export const $: (
        strings: TemplateStringsArray,
        ...values: any[]
    ) => {
        quiet(): Promise<unknown>
    }

    export class S3Client {
        static file(path: string, options: Record<string, any>): any
        static list(
            options: Bun.S3ListObjectsOptions | undefined,
            config: Record<string, any>
        ): Promise<Bun.S3ListObjectsResponse>
        static write(path: string, data: string, config: Record<string, any>): Promise<void>
        static delete(path: string, config: Record<string, any>): Promise<void>
    }

    export class RedisClient {
        connected: boolean
        onconnect?: () => void
        onclose?: (err: Error) => void

        constructor(url: string, options?: Record<string, any>)

        connect(): void
        send(command: string, args: string[]): Promise<any>
        publish(channel: string, message: string): Promise<unknown>
        subscribe(channel: string, listener: (message: string) => void): Promise<void>
    }
}

declare module 'bun:sqlite' {
    export class Database {
        constructor(filename: string, options?: Record<string, any>)
        exec(sql: string): void
        close(): void
        query(sql: string): {
            run(...args: any[]): any
            get(...args: any[]): any
            all(...args: any[]): any[]
        }
        transaction<T extends (...args: any[]) => any>(fn: T): T
    }
}
