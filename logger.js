const fs = require('fs');
const path = require('path');
const { formatTs } = require('./utils');

class Logger {
  constructor(config) {
    this.maxMemoryLogs = config.log.maxMemoryLogs;
    this.saveToFile = config.log.saveToFile;
    this.logs = [];
    this.logDir = path.resolve(process.cwd(), 'logs');

    if (this.saveToFile && !fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  push(level, type, message, context = {}) {
    const item = {
      timestamp: Date.now(),
      time: formatTs(Date.now()),
      level,
      type,
      message,
      context,
    };

    this.logs.unshift(item);
    if (this.logs.length > this.maxMemoryLogs) {
      this.logs = this.logs.slice(0, this.maxMemoryLogs);
    }

    if (this.saveToFile) {
      const fileName = `${new Date().toISOString().slice(0, 10)}.log`;
      const filePath = path.join(this.logDir, fileName);
      fs.appendFile(filePath, `${JSON.stringify(item)}\n`, () => {});
    }

    if (level === 'error') {
      // eslint-disable-next-line no-console
      console.error(`[${item.time}] [${type}] ${message}`, context);
    } else {
      // eslint-disable-next-line no-console
      console.log(`[${item.time}] [${type}] ${message}`);
    }

    return item;
  }

  info(type, message, context) {
    return this.push('info', type, message, context);
  }

  warn(type, message, context) {
    return this.push('warning', type, message, context);
  }

  error(type, message, context) {
    return this.push('error', type, message, context);
  }

  signal(message, context) {
    return this.push('info', 'signal', message, context);
  }

  trade(message, context) {
    return this.push('info', 'trade', message, context);
  }

  risk(message, context) {
    return this.push('warning', 'risk', message, context);
  }

  telegram(message, context) {
    return this.push('info', 'telegram', message, context);
  }

  getLogs(limit = 200) {
    return this.logs.slice(0, limit);
  }
}

module.exports = Logger;
