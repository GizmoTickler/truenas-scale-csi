const { ISCSI } = require('./src/utils/iscsi.js');

async function test_success() {
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
        console.log("TEST 1: Valid session ID");
        await iscsi.iscsiadm.rescanSession(123);
        console.log("PASS: Valid session ID worked");
    } catch (e) {
        console.error("FAIL: Valid session ID threw error:", e.message);
        process.exit(1);
    }
}

async function test_failure() {
    const iscsi = new ISCSI({
        executor: {
            spawn: (command, args, options) => { return {}; }
        }
    });

    try {
        console.log("TEST 2: Empty session ID");
        await iscsi.iscsiadm.rescanSession("");
        console.error("FAIL: Empty session ID did not throw error");
        process.exit(1);
    } catch (e) {
        if (e.message === "cannot scan empty session id") {
            console.log("PASS: Empty session ID threw correct error");
        } else {
             console.error("FAIL: Empty session ID threw unexpected error:", e.message);
             process.exit(1);
        }
    }
}

async function run() {
    await test_success();
    await test_failure();
}

run();
