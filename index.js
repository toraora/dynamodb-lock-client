"use strict";

const crypto = require("crypto");
const events = require("events");
const Joi = require("@hapi/joi");
const os = require("os");
const pkg = require("./package.json");
const util = require("util");
const AsyncLock = require('async-lock'); // Local state locks

const FailClosed = function(config)
{
    if(!(this instanceof FailClosed))
    {
        return new FailClosed(config);
    }
    const self = this;

    // constant configuration
    self._config = config;

    const configValidationResult = FailClosed.schema.config.validate(
        self._config,
        {
            abortEarly: false,
            convert: false
        }
    );
    if (configValidationResult.error)
    {
        throw configValidationResult.error;
    }
    self._retryCount = self._config.retryCount === undefined ? 1 : self._config.retryCount;
};

FailClosed.schema =
{
    config: require("./schema/failClosedConfig.js")
};

FailClosed.prototype.acquireLock = function(id, callback)
{
    const self = this;
    let partitionID, sortID;
    if (typeof id === "object")
    {
        partitionID = id[self._config.partitionKey];
        sortID = id[self._config.sortKey];
    }
    else
    {
        partitionID = id;
    }
    if (self._config.sortKey && sortID === undefined)
    {
        return callback(new Error("Lock ID is missing required sortKey value"));
    }
    const workflow = new events.EventEmitter();
    // Register workflow event handlers
    workflow.on("start", dataBag => workflow.emit("acquire lock", dataBag));
    workflow.on("acquire lock", dataBag =>
        {
            const params =
            {
                TableName: self._config.lockTable,
                Item:
                {
                    [self._config.partitionKey]: dataBag.partitionID,
                    owner: dataBag.owner,
                    guid: dataBag.guid
                },
                ConditionExpression: buildAttributeNotExistsExpression(self),
                ExpressionAttributeNames: buildExpressionAttributeNames(self)
            };
            if (self._config.sortKey)
            {
                params.Item[self._config.sortKey] = dataBag.sortID;
            }
            self._config.dynamodb.put(params, (error, data) =>
                {
                    if (error)
                    {
                        if (error.code === "ConditionalCheckFailedException")
                        {
                            if (dataBag.retryCount > 0)
                            {
                                return workflow.emit("retry acquire lock", dataBag);
                            }
                            else
                            {
                                const err = new Error("Failed to acquire lock.");
                                err.code = "FailedToAcquireLock";
                                err.originalError = error;
                                return callback(err);
                            }
                        }
                        return callback(error);
                    }
                    return callback(undefined, new Lock(
                        {
                            dynamodb: self._config.dynamodb,
                            guid: dataBag.guid,
                            lockTable: self._config.lockTable,
                            partitionID: dataBag.partitionID,
                            partitionKey: self._config.partitionKey,
                            sortID: dataBag.sortID,
                            sortKey: self._config.sortKey,
                            type: FailClosed
                        }
                    ));
                }
            );
        }
    );
    workflow.on("retry acquire lock", dataBag =>
        {
            dataBag.retryCount--;
            setTimeout(() => workflow.emit("acquire lock", dataBag), self._config.acquirePeriodMs);
        }
    );
    // Start the workflow
    workflow.emit("start",
        {
            partitionID,
            sortID,
            owner: self._config.owner || `${pkg.name}@${pkg.version}_${os.userInfo().username}@${os.hostname()}`,
            retryCount: self._retryCount,
            guid: crypto.randomBytes(64)
        }
    )
};

function buildAttributeExistsExpression(self)
{
    let expr = "attribute_exists(#partitionKey)";
    if (self._config.sortKey)
    {
        expr += " and attribute_exists(#sortKey)";
        return `(${expr})`;
    }
    return expr;
}

function buildAttributeNotExistsExpression(self)
{
    let expr = "attribute_not_exists(#partitionKey)";
    if (self._config.sortKey)
    {
        expr += " and attribute_not_exists(#sortKey)";
        return `(${expr})`;
    }
    return expr;
}

function buildExpressionAttributeNames(self)
{
    const names =
    {
        "#partitionKey": self._config.partitionKey
    };
    if (self._config.sortKey)
    {
        names["#sortKey"] = self._config.sortKey;
    }
    return names;
}

const FailOpen = function(config)
{
    if(!(this instanceof FailOpen))
    {
        return new FailOpen(config);
    }
    const self = this;

    // constant configuration
    self._config = config;

    const configValidationResult = FailOpen.schema.config.validate(
        self._config,
        {
            abortEarly: false,
            convert: false
        }
    );
    if (configValidationResult.error)
    {
        throw configValidationResult.error;
    }
    self._retryCount = self._config.retryCount === undefined ? 1 : self._config.retryCount;
};

FailOpen.schema =
{
    config: require("./schema/failOpenConfig.js")
};

FailOpen.prototype.acquireLock = function(id, callback)
{
    const self = this;
    let partitionID, sortID;
    if (typeof id === "object")
    {
        partitionID = id[self._config.partitionKey];
        sortID = id[self._config.sortKey];
    }
    else
    {
        partitionID = id;
    }
    if (self._config.sortKey && sortID === undefined)
    {
        return callback(new Error("Lock ID is missing required sortKey value"));
    }
    const workflow = new events.EventEmitter();
    // Register workflow event handlers
    workflow.on("start", dataBag => workflow.emit("check for existing lock", dataBag));
    workflow.on("check for existing lock", dataBag =>
        {
            const params =
            {
                TableName: self._config.lockTable,
                Key:
                {
                    [self._config.partitionKey]: dataBag.partitionID
                },
                ConsistentRead: true
            };
            if (self._config.sortKey)
            {
                params.Key[self._config.sortKey] = dataBag.sortID;
            }
            self._config.dynamodb.get(params, (error, data) =>
                {
                    if (error)
                    {
                        return callback(error);
                    }
                    if (!data.Item)
                    {
                        dataBag.fencingToken = 1;
                        return workflow.emit("acquire new lock", dataBag);
                    }
                    dataBag.lock = data.Item;
                    dataBag.fencingToken = dataBag.lock.fencingToken + 1;
                    const leaseDurationMs = parseInt(dataBag.lock.leaseDurationMs);

                    // Check if there's already a non-stale waiter
                    if (self._config.failIfContested && dataBag.lock.waiterSince)
                    {
                        const waiterAge = Date.now() - dataBag.lock.waiterSince;
                        const maxWaitTime = leaseDurationMs * 2;
                        if (waiterAge < maxWaitTime)
                        {
                            const err = new Error("Lock already has a waiter.");
                            err.code = "LockContested";
                            return callback(err);
                        }
                    }

                    let timeout;
                    if (self._config.trustLocalTime)
                    {
                        const lockAcquiredTimeUnixMs = parseInt(dataBag.lock.lockAcquiredTimeUnixMs);
                        const localTimeUnixMs = (new Date()).getTime();
                        timeout = Math.max(0, leaseDurationMs - (localTimeUnixMs - lockAcquiredTimeUnixMs));
                    }
                    else
                    {
                        timeout = leaseDurationMs;
                    }

                    // Signal that we're waiting
                    return workflow.emit("register waiter", dataBag, timeout);
                }
            );
        }
    );
    workflow.on("register waiter", (dataBag, timeout) =>
        {
            const updateWaiterTimestamp = () =>
            {
                const updateParams =
                {
                    TableName: self._config.lockTable,
                    Key:
                    {
                        [self._config.partitionKey]: dataBag.partitionID
                    },
                    UpdateExpression: "SET waiterSince = :now",
                    ExpressionAttributeValues:
                    {
                        ":now": Date.now()
                    }
                };
                if (self._config.sortKey)
                {
                    updateParams.Key[self._config.sortKey] = dataBag.sortID;
                }
                self._config.dynamodb.update(updateParams, () => {});
            };

            // Set initial waiter timestamp
            updateWaiterTimestamp();

            // Start waiter heartbeat to keep waiterSince fresh
            const leaseDurationMs = parseInt(self._config.leaseDurationMs);
            dataBag.waiterHeartbeat = setInterval(updateWaiterTimestamp, leaseDurationMs / 2);

            // Wait for lease to expire then try to acquire
            setTimeout(
                () => workflow.emit("acquire existing lock", dataBag),
                timeout
            );
        }
    );
    workflow.on("acquire new lock", dataBag =>
        {
            const params =
            {
                TableName: self._config.lockTable,
                Item:
                {
                    [self._config.partitionKey]: dataBag.partitionID,
                    fencingToken: dataBag.fencingToken,
                    leaseDurationMs: self._config.leaseDurationMs,
                    owner: dataBag.owner,
                    guid: dataBag.guid
                },
                ConditionExpression: buildAttributeNotExistsExpression(self),
                ExpressionAttributeNames: buildExpressionAttributeNames(self)
            };
            if (self._config.trustLocalTime)
            {
                params.Item.lockAcquiredTimeUnixMs = (new Date()).getTime();
            }
            if (dataBag.sortID)
            {
                params.Item[self._config.sortKey] = dataBag.sortID;
            }
            self._config.dynamodb.put(params, (error, data) =>
                {
                    if (error)
                    {
                        if (error.code === "ConditionalCheckFailedException")
                        {
                            if (dataBag.retryCount > 0)
                            {
                                dataBag.retryCount--;
                                return workflow.emit("check for existing lock", dataBag);
                            }
                            else
                            {
                                const err = new Error("Failed to acquire lock.");
                                err.code = "FailedToAcquireLock";
                                err.originalError = error;
                                return callback(err);
                            }
                        }
                        return callback(error);
                    }
                    return workflow.emit("configure acquired lock", dataBag);
                }
            );
        }
    );
    workflow.on("acquire existing lock", dataBag =>
        {
            // Clear waiter heartbeat since we're attempting to acquire
            if (dataBag.waiterHeartbeat)
            {
                clearInterval(dataBag.waiterHeartbeat);
                dataBag.waiterHeartbeat = undefined;
            }

            const params =
            {
                TableName: self._config.lockTable,
                Item:
                {
                    [self._config.partitionKey]: dataBag.partitionID,
                    fencingToken: dataBag.fencingToken,
                    leaseDurationMs: self._config.leaseDurationMs,
                    owner: dataBag.owner,
                    guid: dataBag.guid
                    // waiterSince intentionally omitted - put replaces entire item
                },
                ConditionExpression: `${buildAttributeNotExistsExpression(self)} or (guid = :guid and fencingToken = :fencingToken)`,
                ExpressionAttributeNames: buildExpressionAttributeNames(self),
                ExpressionAttributeValues:
                {
                    ":fencingToken": dataBag.lock.fencingToken,
                    ":guid": dataBag.lock.guid
                }
            };
            if (self._config.trustLocalTime)
            {
                params.Item.lockAcquiredTimeUnixMs = (new Date()).getTime();
            }
            if (dataBag.sortID)
            {
                params.Item[self._config.sortKey] = dataBag.sortID;
            }
            self._config.dynamodb.put(params, (error, data) =>
                {
                    if (error)
                    {
                        if (error.code === "ConditionalCheckFailedException")
                        {
                            if (dataBag.retryCount > 0)
                            {
                                dataBag.retryCount--;
                                return workflow.emit("check for existing lock", dataBag);
                            }
                            else
                            {
                                const err = new Error("Failed to acquire lock.");
                                err.code = "FailedToAcquireLock";
                                err.originalError = error;
                                return callback(err);
                            }
                        }
                        return callback(error);
                    }
                    return workflow.emit("configure acquired lock", dataBag);
                }
            );
        }
    );
    workflow.on("configure acquired lock", dataBag =>
        {
            return callback(undefined, new Lock(
                {
                    dynamodb: self._config.dynamodb,
                    fencingToken: dataBag.fencingToken,
                    guid: dataBag.guid,
                    heartbeatPeriodMs: self._config.heartbeatPeriodMs,
                    maximumDurationMs: self._config.maximumDurationMs,
                    errorOnHeartbeatFailure: self._config.errorOnHeartbeatFailure,
                    leaseDurationMs: self._config.leaseDurationMs,
                    lockTable: self._config.lockTable,
                    owner: dataBag.owner,
                    partitionID: dataBag.partitionID,
                    partitionKey: self._config.partitionKey,
                    sortID: dataBag.sortID,
                    sortKey: self._config.sortKey,
                    trustLocalTime: self._config.trustLocalTime,
                    type: FailOpen
                }
            ));
        }
    );
    // Start the workflow
    workflow.emit("start",
        {
            partitionID,
            sortID,
            owner: self._config.owner || `${pkg.name}@${pkg.version}_${os.userInfo().username}@${os.hostname()}`,
            retryCount: self._retryCount,
            guid: crypto.randomBytes(64)
        }
    )
};

const Lock = function(config)
{
    const self = this;
    events.EventEmitter.call(self);

    self._firstAcquisitionTime = (new Date()).getTime();

    // constant Lock configuration
    self._config = config;

    // variable properties
    self._guid = self._config.guid;
    self._released = false;

    // Race conditions are created with timeout processes (`refresh` for HB) and other lock operations 
    // like `release`
    self._lock = new AsyncLock();
    self._lockId = `${self._config.partitionID}:${self._config.sortID || ''}`;

    // public properties
    self.fencingToken = self._config.fencingToken;

    if (self._config.heartbeatPeriodMs)
    {
        const refreshLock = function()
        {
            self._lock.acquire(self._lockId, () => {
                
                // refuse to refresh lock after maximumDurationMs
                const now = (new Date()).getTime();
                if (self._config.maximumDurationMs && (now - self._firstAcquisitionTime > self._config.maximumDurationMs)) {
                    return;
                } else if (self._released) {
                    return;
                }
    
                const newGuid = crypto.randomBytes(64);
                const params =
                {
                    TableName: self._config.lockTable,
                    Item:
                    {
                        [self._config.partitionKey]: self._config.partitionID,
                        fencingToken: self._config.fencingToken,
                        leaseDurationMs: self._config.leaseDurationMs,
                        owner: self._config.owner,
                        guid: newGuid
                    },
                    ConditionExpression: `${buildAttributeExistsExpression(self)} and guid = :guid`,
                    ExpressionAttributeNames: buildExpressionAttributeNames(self),
                    ExpressionAttributeValues:
                    {
                        ":guid": self._guid
                    }
                };
                if (self._config.trustLocalTime)
                {
                    params.Item.lockAcquiredTimeUnixMs = (new Date()).getTime();
                }
                if (self._config.sortKey)
                {
                    params.Item[self._config.sortKey] = self._config.sortID;
                }
                self._config.dynamodb.put(params, (error, data) =>
                    {
                        if (error)
                        {
                            if (self._config.errorOnHeartbeatFailure) {
                                return self.emit("error", error);
                            }
                            return;
                        }
                        self._guid = newGuid;
                        if (!self._released) // See https://github.com/tristanls/dynamodb-lock-client/issues/1
                        {
                            self.heartbeatTimeout = setTimeout(refreshLock, self._config.heartbeatPeriodMs);
                        }
                    }
                );
            });
        };
        self.heartbeatTimeout = setTimeout(refreshLock, self._config.heartbeatPeriodMs);
    }
};

util.inherits(Lock, events.EventEmitter);

Lock.prototype.release = function(callback)
{
    this._lock.acquire(this._lockId, () => {
        const self = this;
        self._released = true;
        if (self.heartbeatTimeout)
        {
            clearTimeout(self.heartbeatTimeout);
            self.heartbeatTimeout = undefined;
        }
        if (self._config.type == FailOpen)
        {
            return self._releaseFailOpen(callback);
        }
        else
        {
            return self._releaseFailClosed(callback);
        }
    });
};

Lock.prototype._releaseFailClosed = function(callback)
{
    const self = this;
    const params =
    {
        TableName: self._config.lockTable,
        Key:
        {
            [self._config.partitionKey]: self._config.partitionID
        },
        ConditionExpression: `${buildAttributeExistsExpression(self)} and guid = :guid`,
        ExpressionAttributeNames: buildExpressionAttributeNames(self),
        ExpressionAttributeValues:
        {
            ":guid": self._guid
        }
    };
    if (self._config.sortKey)
    {
        params.Key[self._config.sortKey] = self._config.sortID;
    }
    self._config.dynamodb.delete(params, (error, data) =>
        {
            if (error && error.code === "ConditionalCheckFailedException")
            {
                const err = new Error("Failed to release lock.");
                err.code = "FailedToReleaseLock";
                err.originalError = error;
                return callback(err);
            }
            return callback(error);
        }
    );
};

Lock.prototype._releaseFailOpen = function(callback)
{
    const self = this;
    const params =
    {
        TableName: self._config.lockTable,
        Item:
        {
            [self._config.partitionKey]: self._config.partitionID,
            fencingToken: self._config.fencingToken,
            leaseDurationMs: 1,
            owner: self._config.owner,
            guid: self._guid
        },
        ConditionExpression: `${buildAttributeExistsExpression(self)} and guid = :guid`,
        ExpressionAttributeNames: buildExpressionAttributeNames(self),
        ExpressionAttributeValues:
        {
            ":guid": self._guid
        }
    };
    if (self._config.trustLocalTime)
    {
        params.Item.lockAcquiredTimeUnixMs = (new Date()).getTime();
    }
    if (self._config.sortKey)
    {
        params.Item[self._config.sortKey] = self._config.sortID;
    }
    self._config.dynamodb.put(params, (error, data) =>
        {
            if (error && error.code === "ConditionalCheckFailedException")
            {
                // another process may have claimed lock already
                return callback();
            }
            return callback(error);
        }
    );

};

module.exports =
{
    FailClosed,
    FailOpen,
    Lock
};
