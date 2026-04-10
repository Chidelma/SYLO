declare const process: {
    env: Record<string, string | undefined>
    cwd(): string
}

declare const Buffer: {
    alloc(size: number): Uint8Array & {
        toString(encoding?: string): string
    }
}

declare namespace NodeJS {
    interface ErrnoException extends Error {
        code?: string
    }
}

declare module 'node:path' {
    const path: {
        join(...parts: string[]): string
        dirname(target: string): string
        basename(target: string, suffix?: string): string
    }

    export default path
}

declare module 'node:crypto' {
    export function createHash(algorithm: string): {
        update(value: string): {
            digest(encoding: 'hex'): string
        }
    }
}

declare module 'node:fs/promises' {
    export function mkdir(path: string, options?: { recursive?: boolean }): Promise<void>
    export function readFile(path: string, encoding: string): Promise<string>
    export function readdir(
        path: string,
        options?: { withFileTypes?: boolean }
    ): Promise<
        Array<{
            name: string
            isDirectory(): boolean
        }>
    >
    export function rm(
        path: string,
        options?: { recursive?: boolean; force?: boolean }
    ): Promise<void>
    export function stat(path: string): Promise<{ size: number }>
    export function writeFile(path: string, data: string, encoding: string): Promise<void>
    export function open(
        path: string,
        flags: string
    ): Promise<{
        write(data: string): Promise<void>
        read(buffer: Uint8Array, offset: number, length: number, position: number): Promise<void>
        close(): Promise<void>
    }>
}
