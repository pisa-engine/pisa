declare const Commit: {
    do(repoPath: string, version: string): Promise<void>;
};
export default Commit;
