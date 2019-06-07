
/* IMPORT */

import * as _ from 'lodash';
import * as path from 'path';
import * as pify from 'pify';
import * as semver from 'semver';
import * as simpleGit from 'simple-git';
import {Bump, Commit} from '../../types';
import Utils from '../../utils';

/* ABSTRACT */

abstract class Abstract {

  repoPath; git;

  constructor ( repoPath: string ) {

    this.repoPath = repoPath;

    this.git = pify ( _.bindAll ( simpleGit ( this.repoPath ), ['log', 'silent', 'show'] ) );
    this.git.silent ( true );

    this.init ();

  }

  abstract async isSupported (): Promise<boolean>;

  init () {}

  async bump ( increment: string, version: string | null = null ) {

    /* VERSION */

    if ( !version ) {

      version = await this.getVersion ();

      version = semver.inc ( version, increment );

      if ( !version ) return;

    }

    /* BUMP */

    await this.updateVersion ( version );

  }

  async getContent ( filePath: string ) {

    const repoFilePath = path.join ( this.repoPath, filePath );

    return await Utils.file.read ( repoFilePath );

  }

  async getContentByCommit ( commit: Commit, filePath: string ) {

    try {

      return await this.git.show ( `${commit.hash}:${filePath}` );

    } catch ( e ) {}

  }

  async setContent ( filePath: string, content: string ) {

    const repoFilePath = path.join ( this.repoPath, filePath );

    await Utils.file.write ( repoFilePath, content );

  }

  async getCommitNth ( nth: number ): Promise<Commit | undefined> {

    const commits = await this.getCommitsChunk ( nth, 1 );

    return commits[0];

  }

  async getCommitsChunk ( nth: number, size: number ): Promise<Commit[]> {

    try { // An error gets thrown if there are no commits

      const log = await this.git.log ([ '-n', size, '--skip', size * nth ]);

      return log.all;

    } catch ( e ) {

      return [];

    }

  }

  async getCommitsBumps ( limit: number = Infinity ): Promise<Bump[]> { // Get "limit" number of groups of commits grouped by version

    let bumps: Bump[] = [],
        bump: Bump = { version: '', commits: [] },
        chunkNth = 0,
        prevVersion;

    whileloop:
    while ( true ) {

      const commits = await this.getCommitsChunk ( chunkNth, 50 );

      if ( !commits.length ) break;

      for ( let commit of commits ) {

        const version = await this.getVersionByCommit ( commit );

        if ( version !== prevVersion ) {

          const commitLast = bump.commits.pop () as Commit;

          if ( bumps.length >= limit ) break whileloop;

          const commits = [commit];

          if ( commitLast ) {

            commits.unshift ( commitLast );

            commitLast.isBump = true;

          }

          if ( !bump.commits.length ) {

            bumps.pop ();

          }

          bump = { version: prevVersion || version, commits };

          bumps.push ( bump );

        } else {

          if ( !bump.version ) bump.version = version;

          bump.commits.push ( commit );

        }

        prevVersion = version;

      }

      chunkNth++;

    }

    return bumps;

  }

  async getVersion () {

    const commit = await this.getCommitNth ( 0 );

    return await this.getVersionByCommit ( commit );

  }

  abstract async getVersionByCommit ( commit?: Commit ): Promise<string>;

  abstract async updateVersion ( version: string );

}

/* EXPORT */

export default Abstract;
