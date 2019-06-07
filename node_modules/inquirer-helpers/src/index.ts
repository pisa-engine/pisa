
/* IMPORT */

import isPlainObject = require ( 'lodash/isPlainObject' );
import isString = require ( 'lodash/isString' );
import isUndefined = require ( 'lodash/isUndefined' );
import max = require ( 'lodash/max' );
import padStart = require ( 'lodash/padStart' );
import padEnd = require ( 'lodash/padEnd' );
import sum = require ( 'lodash/sum' );
import * as chalk from 'chalk';
import * as inquirer from 'inquirer';
import * as truncate from 'cli-truncate';
import * as windowSize from 'window-size';

/* INQUIRER HELPERS */

const InquirerHelpers = {

  /* VARIABLES */

  FULLSCREEN: true,
  PAGE_SIZE: 10,
  CLI_WIDTH: 80,

  /* HELPERS */

  _cliWidth () {

    const size = windowSize.get ();

    return size ? size.width : InquirerHelpers.CLI_WIDTH;

  },

  _cliPageSize () {

    const size = windowSize.get (),
          height = size ? size.height : InquirerHelpers.PAGE_SIZE + 2;

    return Math.min ( height - 2, InquirerHelpers.FULLSCREEN ? Infinity : InquirerHelpers.PAGE_SIZE );

  },

  /* API */

  async confirm ( message: string, fallback: boolean = false ) {

    const {result} = await inquirer.prompt ({
      type: 'confirm',
      name: 'result',
      message,
      default: fallback
    });

    return !!result;

  },

  async noYes ( message: string ) {

    return await InquirerHelpers.list ( message, ['No', 'Yes'] ) === 'Yes';

  },

  async yesNo ( message: string ) {

    return await InquirerHelpers.list ( message, ['Yes', 'No'] ) === 'Yes';

  },

  async input ( message: string, fallback? ) {

    const {result} = await inquirer.prompt ({
      type: 'input',
      name: 'result',
      message,
      default: fallback,
      validate: x => !isUndefined ( fallback ) || ( isString ( x ) && !!x.trim () )
    });

    return result;

  },

  async list ( message: string, list: any[], fallback? ) { //FIXME: list should be string[] | object[]

    /* TRUNCATE */

    const maxWidth = InquirerHelpers._cliWidth () - 3; // Accounting for inquirer's characters

    list.map ( entry => {
      if ( isString ( entry ) ) {
        return truncate ( entry.trim (), maxWidth );
      } else if ( isPlainObject ( entry ) && entry.name ) {
        entry.name = truncate ( entry.name.trim (), maxWidth );
      }
      return entry;
    });

    /* END OF LIST */

    const pageSize = InquirerHelpers._cliPageSize ();

    if ( list.length > pageSize ) list.push ( new inquirer.Separator ( '\n' ) );

    /* LIST */

    const {result} = await inquirer.prompt ({
      type: 'list',
      name: 'result',
      choices: list,
      pageSize,
      message,
      default: fallback,
      validate: x => !isUndefined ( fallback ) || ( isString ( x ) && x.trim () )
    });

    return result;

  },

  async checkbox ( message: string, list: any[], fallback? ) {

    /* TRUNCATE */

    const maxWidth = InquirerHelpers._cliWidth () - 3; // Accounting for inquirer's characters

    list.map ( entry => {
      if ( isString ( entry ) ) {
        return truncate ( entry.trim (), maxWidth );
      } else if ( isPlainObject ( entry ) && entry.name ) {
        entry.name = truncate ( entry.name.trim (), maxWidth );
      }
      return entry;
    });

    /* END OF LIST */

    const pageSize = InquirerHelpers._cliPageSize ();

    if ( list.length > pageSize ) list.push ( new inquirer.Separator ( '\n' ) );

    /* LIST */

    const {result} = await inquirer.prompt ({
      type: 'checkbox',
      name: 'result',
      choices: list,
      pageSize,
      message,
      default: fallback,
      validate: x => !isUndefined ( fallback ) || !!x.length
    });

    return result;

  },

  async table ( message: string, table: string[][], values: any[], colors: any[] = [], fallback? ) {

    /* TRUNCATE */

    const maxWidth = InquirerHelpers._cliWidth () - 7; // Accounting for inquirer's characters and table's characters

    table.map ( row => [truncate ( row[0], maxWidth ), ...row.slice ( 1 )] );

    /* FORMATTING */

    if ( table[0].length > 1 ) {

      /* MAX LENGHTS  */

      const maxLenghts = table[0].map ( ( val, index ) => max ( table.map ( row => String ( row[index] ).length ) ) ),
            overflowColumn = maxLenghts.findIndex ( ( length, index ) => ( sum ( maxLenghts.slice ( 0, index + 1 ) ) + ( index * 4 ) ) > maxWidth ),
            maxColumn = overflowColumn >= 0 ? Math.max ( 0, overflowColumn - 1 ) : maxLenghts.length - 1;

      /* FILTERING */

      table = table.map ( row => row.slice ( 0, maxColumn + 1) );

      /* PADDING */

      table = table.map ( row => {
        return row.map ( ( val, index ) => {
          const padFN = index > 0 ? padStart : padEnd;
          return padFN ( val, maxLenghts[index] );
        });
      });

      /* COLORIZE */

      if ( colors.length ) {

        table = table.map ( row => {
          return row.map ( ( val, index ) => {
            const color = colors[index];
            if ( !color ) return val;
            return chalk[color]( val );
          });
        });

      }

    }

    /* LIST */

    const list = table.map ( ( row, index ) => ({
      name: row.length > 1 ? `| ${row.join ( ' | ' )} |` : row[0],
      short: row[0].trim (),
      value: values[index]
    }));

    return await InquirerHelpers.list ( message, list, fallback );

  }

};

/* EXPORT */

export default InquirerHelpers;
