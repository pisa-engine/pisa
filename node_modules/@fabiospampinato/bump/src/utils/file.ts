
/* IMPORT */

import * as fs from 'fs';
import * as mkdirp from 'mkdirp';
import * as path from 'path';
import * as pify from 'pify';
import * as touch from 'touch';

/* FILE */

const File = {

  async exists ( filePath: string ) {

    try {

      await pify ( fs.access )( filePath );

      return true;

    } catch ( e ) {

      return false;

    }

  },

  async make ( filePath: string, content: string ) {

    await pify ( mkdirp )( path.dirname ( filePath ) );

    return File.write ( filePath, content );

  },

  async read ( filePath: string ) {

    try {

      return ( await pify ( fs.readFile )( filePath, { encoding: 'utf8' } ) ).toString ();

    } catch ( e ) {}

  },

  async write ( filePath: string, content: string ) {

    await pify ( fs.writeFile )( filePath, content, {} );

    touch.sync ( filePath ); // So that programs will notice the change

  }

};

/* EXPORT */

export default File;
