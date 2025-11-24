const { ISCSI } = require('./src/utils/iscsi.js');

async function test() {
    const iscsi = new ISCSI({
        executor: {
            spawn: (command, args, options) => {
                console.log(`Mock spawn: ${command} ${args.join(' ')}`);
                // Return a mock child process
                const EventEmitter = require('events');
                const child = new EventEmitter();
                child.stdout = new EventEmitter();
                child.stderr = new EventEmitter();
                setTimeout(() => {
                    child.emit('close', 0);
                }, 10);
                return child;
            }
        }
    });

    try {
        console.log("Attempting to rescan session with ID 123...");
        await iscsi.iscsiadm.rescanSession(123);
        console.log("Success!");
    } catch (e) {
        console.error("Caught error:", e.message);
    }
}

test();
