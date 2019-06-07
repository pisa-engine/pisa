
/* IMPORT */

import Tag from '../providers/tag/git';
import Utils from '../utils';

/* TAG */

async function tag () {

  const repoPath = await Utils.repository.getPath (),
        version = await Utils.repository.getVersion ( repoPath );

  if ( !repoPath || !version ) return Utils.exit ( '[tag] Unsupported repository' );

  await Utils.script.run ( 'pretag' );

  Utils.log ( 'Tagging the commit...' );

  await Tag.add ( repoPath, version );

  await Utils.script.run ( 'posttag' );

}

/* EXPORT */

export default tag;
