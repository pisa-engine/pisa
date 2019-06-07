
/* TYPES */

type Bump = {
  commits: Commit[]
  version: string
};

type Commit = {
  author_email: string,
  author_name: string,
  date: string,
  hash: string,
  message: string,
  isBump?: boolean
};

type Options = {
  version?: boolean,
  changelog?: boolean,
  commit?: boolean,
  tag?: boolean,
  release?: boolean
};

/* EXPORT */

export {Bump, Commit, Options};
