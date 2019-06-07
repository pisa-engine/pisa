
/* IMPORT */

import Commit from '../providers/commit/git';
import Utils from '../utils';

/* COMMIT */

async function commit () {

  const repoPath = await Utils.repository.getPath (),
        version = await Utils.repository.getVersion ( repoPath );

  if ( !repoPath || !version ) return Utils.exit ( '[commit] Unsupported repository' );

  await Utils.script.run ( 'precommit' );

  Utils.log ( 'Making the commit...' );

  await Commit.do ( repoPath, version );

  await Utils.script.run ( 'postcommit' );

}

/* EXPORT */

export default commit;
