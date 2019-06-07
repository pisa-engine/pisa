declare const File: {
    exists(filePath: string): Promise<boolean>;
    make(filePath: string, content: string): Promise<void>;
    read(filePath: string): Promise<any>;
    write(filePath: string, content: string): Promise<void>;
};
export default File;
