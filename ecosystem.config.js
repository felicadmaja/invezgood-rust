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
            // Semua stdout/stderr app → satu file di root workspace.
            // build.sh mengosongkan file ini setiap deploy ulang.
            out_file: './stockbit_ws.log',
            error_file: './stockbit_ws.log',
            merge_logs: true,
            time: true,
        },
    ],
};
