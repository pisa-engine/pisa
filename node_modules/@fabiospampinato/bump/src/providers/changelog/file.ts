
/* IMPORT */

import * as _ from 'lodash';
import * as moment from 'moment';
import * as opn from 'opn';
import * as path from 'path';
import Prompt from 'inquirer-helpers';
import * as stringMatches from 'string-matches';
import {Commit} from '../../types';
import Config from '../../config';
import Utils from '../../utils';

/* CHANGELOG */

const Changelog = {

  async open ( repoPath: string ) {

    const changelogPath = path.join ( repoPath, Config.changelog.file );

    if ( !await Utils.file.exists ( changelogPath ) ) return;

    opn ( changelogPath, { wait: false } );

  },

  async init ( repoPath: string ) {

    const bumps = await Utils.repository.getVersionProvidersResult ( repoPath, 'getCommitsBumps' );

    if ( !bumps.length ) return Utils.exit ( '[changelog] No commits found' );

    const sections = bumps.map ( bump => Changelog.section.render ( bump.version, bump.commits ) ),
          content = sections.join ( '' );

    return await Changelog.write ( repoPath, content );

  },

  async read ( repoPath: string ) {

    const changelogPath = path.join ( repoPath, Config.changelog.file ),
          content = await Utils.file.read ( changelogPath );

    return content;

  },

  async update ( repoPath: string, version: string ) {

    const changelogPath = path.join ( repoPath, Config.changelog.file );

    if ( !await Utils.file.exists ( changelogPath ) ) {

      if ( !Config.changelog.create && Config.force ) return;

      if ( Config.changelog.create || await Prompt.noYes ( 'No changelog found, do you want to create it?' ) ) {

        await Changelog.init ( repoPath );

      }

    } else {

      const bumps = await Utils.repository.getVersionProvidersResult ( repoPath, 'getCommitsBumps', 1 );

      if ( !bumps.length ) return;

      const section = Changelog.section.render ( version, bumps[0].commits );

      return Changelog.section.add ( repoPath, section );

    }

  },

  async write ( repoPath: string, content: string ) {

    const changelogPath = path.join ( repoPath, Config.changelog.file ),
          contentCleaned = content.replace ( /^(\s*\r?\n){2,}/gm, '\n' ).replace ( /(\s*\r?\n){2,}$/gm, '\n' ); // Removing multiple starting/ending new lines

    await Utils.file.make ( changelogPath, contentCleaned );

  },

  section: {

    async readLast ( repoPath: string ) {

      const content = await Changelog.read ( repoPath );

      if ( !content ) return;

      const headerRe = new RegExp ( `^${_.escapeRegExp ( Config.changelog.version ).replace ( /\\\[[^\]]+\\\]/gi, '(.*)' )}$`, 'gmi' ), // Regex that matches the headers, assuming they all used the same template
            matches = stringMatches ( content, headerRe, 2 );

      if ( matches.length < 2 ) return;

      return _.trim ( content.slice ( matches[0].index + matches[0][0].length, matches[1].index ) );

    },

    render ( version: string, commits: Commit[] ) {

      /* VARIABLES */

      const lines: string[] = [];

      /* TOKENS */

      const tokens = {
        version,
        version_date: moment ().format ( Config.tokens.version_date.format )
      };

      /* VERSION */

      if ( Config.changelog.version ) {

        lines.push ( Utils.template.render ( Config.changelog.version, tokens ) );

      }

      /* COMMITS */

      if ( Config.changelog.commit ) {

        commits.reverse ().forEach ( commit => {

          if ( commit.isBump ) return;

          const mergeRe = /^(Merge PR|Merge pull request|Merge branch) /gi;

          if ( mergeRe.test ( commit.message ) ) return;

          const {hash, date, message, author_name, author_email} = commit,
                messageCleaned = message.replace ( / \(HEAD\)$/i, '' ).replace ( / \(HEAD -> [^)]+\)$/i, '' ).replace ( / \(tag: [^)]+\)$/i, '' )

          const commitTokens = _.extend ( {}, tokens, {
            date: moment ( date ).format ( Config.tokens.date.format ),
            message: messageCleaned,
            hash,
            hash4: hash.slice ( 0, 4 ),
            hash7: hash.slice ( 0, 7 ),
            hash8: hash.slice ( 0, 8 ),
            author_name,
            author_email
          });

          lines.push ( Utils.template.render ( Config.changelog.commit, commitTokens ) );

        });

      }

      /* SECTION */

      let section = lines.join ( '\n' ) + '\n';

      /* SEPARATOR */

      if ( Config.changelog.separator ) {

        section += Utils.template.render ( Config.changelog.separator, tokens );

      }

      /* RETURN */

      return section;

    },

    async add ( repoPath: string, section: string ) {

      let content = await Changelog.read ( repoPath );

      if ( !content ) return Utils.exit ( '[changelog] Changelog missing' );

      if ( content.startsWith ( section ) ) return; // Duplicate section

      content = section + content;

      await Changelog.write ( repoPath, content );

    }

  }

};

/* EXPORT */

export default Changelog;
