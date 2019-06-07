declare const Repository: {
    getPath(): Promise<string | null>;
    getVersion(repoPath: string | null): Promise<string>;
    getVersionProviders(repoPath: string): Promise<import("../providers/version/files").default[]>;
    getVersionProvidersResult(repoPath: string, method: string, ...args: any[]): Promise<any>;
};
export default Repository;
