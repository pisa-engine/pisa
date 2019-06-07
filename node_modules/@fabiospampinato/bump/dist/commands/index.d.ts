import version from './version';
import changelog from './changelog';
import commit from './commit';
import tag from './tag';
import release from './release';
declare const Commands: {
    version: typeof version;
    changelog: typeof changelog;
    commit: typeof commit;
    tag: typeof tag;
    release: typeof release;
};
export default Commands;
