declare const GitHub: {
    do(repoPath: string, version: string): Promise<void>;
};
export default GitHub;
