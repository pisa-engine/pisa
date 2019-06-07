import { Commit } from '../../types';
declare const Changelog: {
    open(repoPath: string): Promise<void>;
    init(repoPath: string): Promise<void>;
    read(repoPath: string): Promise<any>;
    update(repoPath: string, version: string): Promise<void>;
    write(repoPath: string, content: string): Promise<void>;
    section: {
        readLast(repoPath: string): Promise<string | undefined>;
        render(version: string, commits: Commit[]): string;
        add(repoPath: string, section: string): Promise<void>;
    };
};
export default Changelog;
