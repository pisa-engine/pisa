
/* IMPORT */

import * as _ from 'lodash';

/* CONFIG */

const Config = {

  merge ( object, ...sources ) {

   return _.mergeWith ( object, ...sources, ( prev, next ) => {

     if ( !_.isArray ( prev ) || !_.isArray ( next ) ) return;

     return next;

   });

 }

};

/* EXPORT */

export default Config;
