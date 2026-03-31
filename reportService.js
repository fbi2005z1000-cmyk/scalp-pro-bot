const cron = require('node-cron');

class ReportService {
  constructor(config, logger, statsService, telegramService, stateStore) {
    this.config = config;
    this.logger = logger;
    this.statsService = statsService;
    this.telegramService = telegramService;
    this.stateStore = stateStore;
    this.jobs = [];
  }

  start() {
    this.stop();

    const dailyJob = cron.schedule('58 23 * * *', async () => {
      try {
        const report = this.statsService.buildDailyReport();
        await this.telegramService.sendDailyReport(report);
        this.stateStore.clearDailyStats();
        this.logger.info('report', 'Đã gửi báo cáo ngày');
      } catch (error) {
        this.logger.error('report', 'Lỗi gửi báo cáo ngày', { error: error.message });
      }
    });

    const weeklyJob = cron.schedule('59 23 * * 0', async () => {
      try {
        const report = this.statsService.buildWeeklyReport();
        await this.telegramService.sendWeeklyReport(report);
        this.stateStore.clearWeeklyStats();
        this.logger.info('report', 'Đã gửi báo cáo tuần');
      } catch (error) {
        this.logger.error('report', 'Lỗi gửi báo cáo tuần', { error: error.message });
      }
    });

    this.jobs.push(dailyJob, weeklyJob);
  }

  stop() {
    for (const job of this.jobs) {
      job.stop();
    }
    this.jobs = [];
  }
}

module.exports = ReportService;
