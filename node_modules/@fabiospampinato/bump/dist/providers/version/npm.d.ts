import Files from './files';
declare class NPM extends Files {
    getFiles(): {
        'package.json': string[];
        'package-lock.json': string[];
    };
}
export default NPM;
