
/* IMPORT */

import * as execa from 'execa';
import Config from '../../config';
import Utils from '../../utils';

/* TAG */

const Tag = {

  async add ( repoPath: string, version: string ) {

    const name = Utils.template.render ( Config.tag.name, {version} );

    try {

      await execa ( 'git', ['tag', name], { cwd: repoPath } );

    } catch ( e ) {

      Utils.log ( e );

      Utils.exit ( '[tag] An error occurred while tagging the commit' );

    }

  }

};

/* EXPORT */

export default Tag;
