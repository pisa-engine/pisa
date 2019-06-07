# micromist

Minimal argument parser


# Install

```bash
npm install micromist --save
```

# Usage

```javascript
const micromist = require('micromist');

const args = micromist(process.argv);

console.log("%j", args);
```