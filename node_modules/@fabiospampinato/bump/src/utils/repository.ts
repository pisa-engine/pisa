
/* IMPORT */

import * as _ from 'lodash';
import Config from '../config';
import VersionProviders from '../providers/version';
import Git from './git';

/* REPOSITORY */

const Repository = {

  async getPath (): Promise<string | null> {

    return await Git.getPath ();

  },

  async getVersion ( repoPath: string | null ): Promise<string> {

    if ( repoPath ) {

      const version = await Repository.getVersionProvidersResult ( repoPath, 'getVersion' );

      if ( version ) return version;

    }

    return Config.version.initial;

  },

  async getVersionProviders ( repoPath: string ) {

    const providers = VersionProviders.map ( Provider => new Provider ( repoPath ) ),
          isSupported = await Promise.all ( providers.map ( provider => provider.isSupported () ) ),
          providersSupported = providers.filter ( ( p, index ) => isSupported[index] );

    return providersSupported;

  },

  async getVersionProvidersResult ( repoPath: string, method: string, ...args ) {

    const providers = await Repository.getVersionProviders ( repoPath );

    for ( let provider of providers ) {

      const result = await provider[method]( ...args );

      if ( result ) return result;

    }

  }

};

/* EXPORT */

export default Repository;
