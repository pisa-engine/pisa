
/* IMPORT */

import chalk from 'chalk';

/* EXIT */

function exit ( message: string = 'An error occurred!', code: number = 1 ) {

  console.error ( chalk.red ( message ) );

  process.exit ( code );

}

/* EXPORT */

export default exit;
