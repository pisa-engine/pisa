import { Commit } from '../../types';
import Abstract from './abstract';
declare class Files extends Abstract {
    files: any;
    basePaths: any;
    regexes: any;
    replacements: any;
    isSupported(): Promise<boolean>;
    init(): void;
    getFiles(): {};
    getVersion(): Promise<string>;
    getVersionByFiles(): Promise<string | undefined>;
    getVersionByCommit(commit?: Commit): Promise<string>;
    getVersionByContentProvider(contentProvider: Function): Promise<string | undefined>;
    updateVersion(version: string): Promise<void>;
}
export default Files;
