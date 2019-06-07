
/* IMPORT */

import chalk from 'chalk';
import * as fs from 'fs';
import * as globby from 'globby';
import * as mime from 'mime-types';
import * as octokit from '@octokit/rest';
import * as opn from 'opn';
import * as path from 'path';
import * as username from 'git-username';
import Config from '../../config';
import Utils from '../../utils';
import Changelog from '../changelog/file';

/* COMMIT */

const GitHub = {

  async do ( repoPath: string, version: string ) {

    if ( !Config.release.github.token ) return Utils.exit ( '[release] Missing GitHub token' );

    const cwd = repoPath,
          github = new octokit ({
            auth: Config.release.github.token
          });

    try {

      const owner = Config.release.github.owner || username ({ cwd }),
            repo = Config.release.github.repo || path.basename ( cwd ),
            tag = Utils.template.render ( Config.tag.name, {version} );

      const release = await github.repos.createRelease ({
        owner,
        repo,
        draft: Config.release.github.draft,
        prerelease: Config.release.github.prerelease,
        name: tag,
        tag_name: tag,
        body: await Changelog.section.readLast ( repoPath )
      });

      if ( Config.release.github.open ) {

        opn ( release.data.html_url, { wait: false } );

      }

      if ( Config.release.github.files.length ) {

        const filePaths = await globby ( Config.release.github.files, { cwd, absolute: true } );

        if ( filePaths.length ) {

          Utils.log ( `Uploading ${filePaths.length} files...`);

          for ( let filePath of filePaths ) {

            Utils.log ( `Uploading "${chalk.bold ( filePath )}"` );

            await github.repos.uploadReleaseAsset ({
              url: release.data.upload_url,
              name: path.basename ( filePath ),
              file: fs.createReadStream ( filePath ),
              headers: {
                'content-type': mime.lookup ( filePath ),
                'content-length': fs.statSync ( filePath ).size
              }
            });

          }

        }

      }

    } catch ( e ) {

      Utils.log ( e );

      Utils.exit ( '[release] An error occurred while making the GitHub release' );

    }

  }

};

/* EXPORT */

export default GitHub;
