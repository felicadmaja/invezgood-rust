/**
 * PM2 ecosystem config untuk invezgood
 *
 * Penggunaan:
 *   pm2 start ecosystem.config.js
 *   pm2 stop invezgood
 *   pm2 restart invezgood
 *   pm2 logs invezgood
 *   pm2 monit
 *
 * Deploy (build + restart):
 *   ./build.sh
 */

module.exports = {
    apps: [
        {
            name: 'invezgood',
            script: './target/release/invezgood',
            cwd: '/home/baki1/invezgood_rust',
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
            out_file: './invezgood.log',
            error_file: './invezgood.log',
            merge_logs: true,
            time: true,
        },
    ],
};
