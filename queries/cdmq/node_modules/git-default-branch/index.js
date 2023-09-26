const fs = require('fs');
const path = require('path');
const execSync = require('child_process').execSync;

const getDefaultValue = () => {
	const filename = path.resolve(process.cwd(), '.git/config');
	try {
		return fs.readFileSync(filename, 'utf8');
	} catch (err) {
		return '';
	}
};

const branchExists = (branch) => {
	try {
		execSync(`git show-ref --verify --quiet refs/heads/${branch}`);
		return true;
	} catch (err) {
		return false;
	}
};

module.exports = (config = getDefaultValue()) => {
	if (!config) {
		return '';
	}

	// Check Git config file first, it's faster
	if (config.includes('[branch "main"]')) {
		return 'main';
	}
	if (config.includes('[branch "master"]')) {
		return 'master';
	}

	// Ask Git
	if (branchExists('main')) {
		return 'main';
	}
	if (branchExists('master')) {
		return 'master';
	}

	return '';
};
