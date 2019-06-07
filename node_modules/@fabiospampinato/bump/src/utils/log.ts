
/* IMPORT */

import Config from '../config';

/* EXIT */

function log ( message: string ) {

  if ( Config.silent ) return;

  console.log ( message );

}

/* EXPORT */

export default log;
