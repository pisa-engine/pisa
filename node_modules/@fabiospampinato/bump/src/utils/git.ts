
/* IMPORT */

import * as _ from 'lodash';
import * as findUp from 'find-up';
import * as path from 'path';

/* GIT */

const Git = {

  async getPath (): Promise<string | null> {

    const gitPath = await findUp ( '.git' );

    return _.isString ( gitPath ) ? path.dirname ( gitPath ) : null;

  }

};

/* EXPORT */

export default Git;
