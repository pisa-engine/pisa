
/* IMPORT */

import Config from '../config';
import GitHub from '../providers/release/github';
import Utils from '../utils';

/* RELEASE */

async function release () {

  const repoPath = await Utils.repository.getPath (),
        version = await Utils.repository.getVersion ( repoPath );

  if ( !repoPath || !version ) return Utils.exit ( '[release] Unsupported repository' );

  await Utils.script.run ( 'prerelease' );

  if ( Config.release.github.enabled ) {

    Utils.log ( 'Releasing to GitHub...' );

    await GitHub.do ( repoPath, version );

  }

  await Utils.script.run ( 'postrelease' );

}

/* EXPORT */

export default release;
