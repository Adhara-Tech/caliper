/*
* Licensed under the Apache License, Version 2.0 (the 'License');
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an 'AS IS' BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

'use strict';

const {ConnectorBase, CaliperUtils, ConfigUtil, TxStatus} = require('@hyperledger/caliper-core');

const logger = CaliperUtils.getLogger('eth-gateway-connector');
const fetch = require('node-fetch')
const { v4: uuidv4 } = require('uuid');
/**
 * @typedef {Object} EthereumInvoke
 *
 * @property {string} contract Required. The name of the smart contract
 * @property {string} verb Required. The name of the smart contract function
 * @property {string} args Required. Arguments of the smart contract function in the order in which they are defined
 * @property {boolean} readOnly Optional. If method to call is a view.
 */

/**
 * Extends {BlockchainConnector} for a web3 Ethereum backend.
 */
class EthGatewayConnector extends ConnectorBase {

    /**
     * Create a new instance of the {Ethereum} class.
     * @param {number} workerIndex The zero-based index of the worker who wants to create an adapter instance. -1 for the manager process.
     * @param {string} bcType The target SUT type
     */
    constructor(workerIndex, bcType) {
        super(workerIndex, bcType);

        let configPath = CaliperUtils.resolvePath(ConfigUtil.get(ConfigUtil.keys.NetworkConfig));
        let ethGatewayConfig = require(configPath).gateway;

        // throws on configuration error
        this.checkConfig(ethGatewayConfig);

        this.ethGatewayConfig = ethGatewayConfig;
        this.workerIndex = workerIndex;
        this.context = undefined;
    }

    /**
     * Check the eth-gateway network configuration file for errors, throw if invalid
     * @param {object} ethGatewayConfig The eth-gateway network config to check.
     */
    checkConfig(ethGatewayConfig) {
        if (!ethGatewayConfig.url) {
            throw new Error(
                'No URL given to access the eth-gateway SUT. Please check your network configuration. ' +
                'Please see https://hyperledger.github.io/caliper/v0.3/ethereum-config/ for more info.'
            );
        }
        // if (ethGatewayConfig.url.toLowerCase().indexOf('http') === 0) {
        //     throw new Error(
        //         'Ethereum benchmarks must not use http(s) RPC connections, as there is no way to guarantee the ' +
        //         'order of submitted transactions when using other transports. For more information, please see ' +
        //         'https://github.com/hyperledger/caliper/issues/776#issuecomment-624771622'
        //     );
        // }
    }

    /**
     * Initialize the {Ethereum} object.
     * @param {boolean} workerInit Indicates whether the initialization happens in the worker process.
     * @return {object} Promise<boolean> True if the account got unlocked successful otherwise false.
     */
    init(workerInit) {
        return true;
    }

    /**
     * Deploy smart contracts specified in the network configuration file.
     * @return {object} Promise execution for all the contract creations.
     */
    async installSmartContract() {
    }

    /**
     * It passes deployed contracts addresses to all workers (only known after deploy contract)
     * @param {Number} number of workers to prepare
     * @returns {Array} worker args
     * @async
     */
    async prepareWorkerArguments(number) {
        let result = [];
        for (let i = 0 ; i<= number ; i++) {
            result[i] = {contracts: this.ethGatewayConfig.contracts};
        }
        return result;
    }

    /**
     * Return the eth-gateway context associated with the given callback module name.
     * @param {Number} roundIndex The zero-based round index of the test.
     * @param {object} args worker arguments.
     * @return {object} The assembled eth-gateway context.
     * @async
     */
    async getContext(roundIndex, args) {
        let context = {
            chainId: this.ethGatewayConfig.chainId,
            clientIndex: this.workerIndex,
            contracts: {},
            url: this.ethGatewayConfig.url,
            headers: {
                'Content-Type': 'application/json',
                'X-Auth-Userid': this.ethGatewayConfig.fromUser,
                'X-Auth-ApplicationId': this.ethGatewayConfig.fromApplication,
            },
        };
        for (const key of Object.keys(args.contracts)) {
            context.contracts[key] = {
                id: args.contracts[key].id,
                path: args.contracts[key].path
            };
        }
        this.context = context;
        return context;
    }

    /**
     * Release the given eth-gateway context.
     * @async
     */
    async releaseContext() {
        // nothing to do
    }

    /**
     * Submit a transaction to the eth-gateway context.
     * @param {EthereumInvoke} request Methods call data.
     * @return {Promise<TxStatus>} Result and stats of the transaction invocation.
     */
    async _sendSingleRequest(request) {
        const context = this.context;
        let status = new TxStatus();
        let referenceId = uuidv4().substring(0,8);
        let requestBody = !request.readOnly ?
          {
              txMeta: {
                  executionMode: '',
                  referenceId: referenceId
              },
              arguments: {}
          } :
          {
            arguments: {}
          }
        if (request.hasOwnProperty('args') && request.args) {
            requestBody.arguments = request.args;
        }
        let contractInfo = context.contracts[request.contract];
        let requestPath = contractInfo.path + '/' + request.verb + (request.readOnly ? ':call' : ':sendTx');

        if (request.readOnly) {
            const onFailure = (err) => {
                status.SetStatusFail();
                logger.error('Failed call on contract [' + request.contract + '] calling method [' + request.verb + ']');
                logger.error(err);
            };
            const onSuccess = (rec) => {
                status.SetResult(rec);
                status.SetVerification(true);
                status.SetStatusSuccess();
            };
            try {
                // eslint-disable-next-line no-undef
                const queryResponse = await fetch(new URL(requestPath, context.url), {
                    method: 'POST',
                    headers: context.headers,
                    body: JSON.stringify(requestBody)
                });
                let queryResult = await queryResponse.json();
                onSuccess(queryResult);
            } catch (err) {
                onFailure(err);
            }
            return status;
        } else {
            const onFailure = (err) => {
                status.SetStatusFail();
                logger.error('Failed tx on contract [' + request.contract + '] calling method [' + request.verb + ']');
                logger.error(err);
            };
            const onSuccess = (rec) => {
                status.SetResult(rec);
                status.SetVerification(true);
                status.SetStatusSuccess();
            };
            try {
                // eslint-disable-next-line no-undef
                const submitResponse = await fetch(new URL(requestPath, context.url), {
                    method: 'POST',
                    headers: context.headers,
                    body: JSON.stringify(requestBody)
                });
                let submitResult = await submitResponse.json();
                if (submitResult.error) {
                    onFailure(submitResult.error)
                } else {
                    let state = 'PENDING', queryResult = undefined;
                    //let count = 1
                    await this.sleep(200);
                    while (state === 'PENDING') {
                        // eslint-disable-next-line no-undef
                        const queryResponse = await fetch(new URL('/_services/transactions/' + submitResult.output.referenceId, context.url), {
                            method: 'GET',
                            headers: context.headers,
                        });
                        queryResult = await queryResponse.json();
                        if (queryResult.error) {
                            onFailure(queryResult.error);
                            state = 'FAILED';
                        }
                        state = queryResult.state;
                        if (state === 'SUCCESS') {
                            onSuccess(queryResult);
                        }
                        await this.sleep(500);
                    }
                }
            } catch (err) {
                onFailure(err);
            }
            return status;
        }
    }

    /**
    * Sleep for a bit.
    * @param {int} ms Milliseconds.
    */
    async sleep(ms) {
        return new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    }
}

module.exports = EthGatewayConnector;
