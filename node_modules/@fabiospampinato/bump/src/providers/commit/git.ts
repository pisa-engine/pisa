
/* IMPORT */

import * as execa from 'execa';
import Config from '../../config';
import Utils from '../../utils';

/* COMMIT */

const Commit = {

  async do ( repoPath: string, version: string ) {

    const message = Utils.template.render ( Config.commit.message, {version} );

    try {

      await execa ( 'git', ['add', '.'], { cwd: repoPath } );

      await execa ( 'git', ['commit', '-a', '-m', message], { cwd: repoPath } );

    } catch ( e ) {

      Utils.log ( e );

      Utils.exit ( '[commit] An error occurred while making the commit' );

    }

  }

};

/* EXPORT */

export default Commit;
