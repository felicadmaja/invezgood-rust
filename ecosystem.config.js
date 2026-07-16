/**
 * PM2 ecosystem config untuk stockbit_ws
 *
 * Penggunaan:
 *   pm2 start ecosystem.config.js
 *   pm2 stop stockbit_ws
 *   pm2 restart stockbit_ws
 *   pm2 logs stockbit_ws
 *   pm2 monit
 *
 * Deploy (build + restart):
 *   ./build.sh
 */

module.exports = {
    apps: [
        {
            name: 'stockbit_ws',
            script: './target/release/stockbit_ws',
            cwd: '/home/baki1/stockbit_ws',
            interpreter: 'none',
            instances: 1,
            autorestart: true,
            watch: false,
            max_memory_restart: '1G',
            env: {
                NODE_ENV: 'production',
            },
            out_file: './logs/pm2-stockbit_ws-out.log',
            error_file: './logs/pm2-stockbit_ws-err.log',
            merge_logs: true,
            time: true,
        },
    ],
};
