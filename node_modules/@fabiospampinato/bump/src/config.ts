
/* IMPORT */

import * as _ from 'lodash';
import * as minimist from 'minimist';
import * as os from 'os';
import * as path from 'path';
import Utils from './utils';

/* CONFIG */

const Config = {
  force: false, // Force the command without prompting the user
  silent: false, // Minimize the amount of logs
  files: {}, // A map of `relativeFilePath: [regex, replacement, regexFlags?] | [regex, replacement, regexFlags?][]`
  version: {
    enabled: true, // Bump the version number
    initial: '0.0.0', // Initial version
    increments: ['major', 'minor', 'patch', 'premajor', 'preminor', 'prepatch', 'prerelease', 'custom'] // List of available increments to pick from
  },
  changelog: {
    enabled: true, // Enable changelog auto-updates
    create: false, // Create the changelog file if it doesn't exist
    open: true, // Open the changelog file after bumping
    file: 'CHANGELOG.md', // Name of the changelog file
    version: '### Version [version]', // Template for the version line
    commit: '- [message]', // Template for the commit line
    separator: '\n' // Template for the separator between versions sections
  },
  commit: {
    enabled: true, // Commit the changes automatically
    message: 'Bumped version to [version]' // Template for the commit message
  },
  tag: {
    enabled: true, // Tag the bump commit
    name: 'v[version]' // Template for the name of the tag
  },
  release: {
    enabled: false, // Release to any enabled release providers
    github: {
      enabled: false, // Make a GitHub release
      open: true, // Open the release/draft page
      draft: true, // Mark it as a draft
      prerelease: false, // Mark it as a prerelease
      files: [], // Globs of files to attach to the release
      token: '', // GitHub OAuth token with `public_repo` priviledge
      owner: '', // GitHub repository owner
      repo: '' // GitHub repository name
    }
  },
  tokens: {
    date: {
      format: 'YYYY-MM-DD' // Moment.js format to use when generating the `[date]` token
    },
    version_date: {
      format: 'YYYY-MM-DD' // Moment.js format to use when generating the `[version_date]` token
    }
  },
  scripts: {
    enabled: true, // Run the scripts
    prebump: '', // Script to execute before bumping the version
    postbump: '', // Script to execute after bumping the version
    prechangelog: '', // Script to execute before updating the changelog
    postchangelog: '', // Script to execute after updating the changelog
    precommit: '', // Script to execute before committing
    postcommit: '', // Script to execute after committing
    pretag: '', // Script to execute before tagging
    posttag: '', // Script to execute after tagging
    prerelease: '', // Script to execute before releasing
    postrelease: '' // Script to execute after releasing
  }
};

/* LOCAL */

function initLocal () {

  const localPath = path.join ( os.homedir (), '.bump.json' ),
        localConfig = _.attempt ( require, localPath );

  if ( _.isError ( localConfig ) ) return;

  Utils.config.merge ( Config, localConfig );

}

initLocal ();

/* CWD */

function initCwd () {

  const cwdPath = path.join ( process.cwd (), 'bump.json' ),
        cwdConfig = _.attempt ( require, cwdPath );

  if ( _.isError ( cwdConfig ) ) return;

  Utils.config.merge ( Config, cwdConfig );

}

initCwd ();

/* DYNAMIC */

function initDynamic () {

  const argv = minimist ( process.argv.slice ( 2 ) );

  /* CONFIG */

  const dynamicPath = [argv.config, argv.c].find ( _.isString ),
        dynamicConfigRequire = _.attempt ( require, path.resolve ( process.cwd (), dynamicPath || '' ) ),
        dynamicConfigJSON = _.attempt ( JSON.parse, dynamicPath ),
        dynamicConfig = !_.isError ( dynamicConfigRequire ) ? dynamicConfigRequire : ( !_.isError ( dynamicConfigJSON ) ? dynamicConfigJSON : undefined );

  if ( dynamicConfig ) Utils.config.merge ( Config, dynamicConfig );

  /* COMMIT & TAG */

  if ( !Config.commit.enabled ) Config.tag.enabled = false;

  /* SWITCHES */

  const switches = ['silent', 'force'];

  switches.forEach ( name => {

    Config[name] = [argv[name], Config[name]].find ( _.isBoolean );

  });

  /* SCRIPTS */

  Config.scripts.enabled = [argv.scripts, Config.scripts.enabled].find ( _.isBoolean ) as boolean;

  const scripts = Object.keys ( Config.scripts );

  scripts.forEach ( name => {

    if ( !_.isString ( Config.scripts[name] ) ) return;

    Config.scripts[name] = [argv[name], Config.scripts[name]].find ( _.isString );

  });

}

initDynamic ();

/* INIT ENVIRONMENT */

function initEnvironment () {

  if ( process.env.GITHUB_TOKEN ) Config.release.github.token = process.env.GITHUB_TOKEN;

}

initEnvironment ();

/* EXPORT */

export default Config;
