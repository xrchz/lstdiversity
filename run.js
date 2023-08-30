import { ethers } from 'ethers'
import { program } from 'commander'
import { createWriteStream, readFileSync } from 'node:fs'
import { open } from 'lmdb'

const options = program
  .option('-r, --rpc <url>', 'Full node RPC endpoint URL', 'http://localhost:8545')
  .option('-b, --block <num>', 'Block number to collect data for (default: latest)')
  .option('--prune', 'Prune the database of blocks before <block> then exit')
  .option('--reverse', 'Reverse direction of pruning (delete <block> and greater)')
  .option('-m, --max-query-range <num>', 'Maximum number of blocks to query for events at a time', 1000)
  .option('-c, --multicall-limit <num>', 'Maximum number of balance calls to multicall at a time', 1000)
  .option('-n, --num-top-holders <num>', 'Number of top holders (by total ETH value) to include in output file', 100)
  .option('-f, --filename <name>', 'Name of output file (default: lstdiv-<block>-<tokens+...>-<numTop>.json)')
  .option('-x, --exclude <tokens...>', 'Symbols of LSTs to exclude')
  .parse()
  .opts()

const provider = new ethers.JsonRpcProvider(options.rpc)

const multicall = new ethers.Contract('0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441',
  ['function aggregate((address, bytes)[]) view returns (uint256, bytes[])'], provider)

const db = open({path: 'db'})
// <block>/<lstSymbol> : map from address to balance of holders

const timestamp = () => Intl.DateTimeFormat('en-GB',
  {hour: 'numeric', minute: 'numeric', second: 'numeric'})
  .format(new Date())

const blockTag = options.block ? parseInt(options.block) : await provider.getBlockNumber()
console.log(`${timestamp()} Working @ block ${blockTag}`)

if (options.prune) {
  for (const key of db.getKeys({reverse: options.reverse})) {
    const keyBlock = parseInt(key.split('/')[0])
    if (options.reverse ? keyBlock >= blockTag : keyBlock < blockTag) {
      console.log(`Removing ${key}`)
      await db.remove(key)
    }
    else
      break
  }
  process.exit()
}

const oneEther = ethers.parseEther('1')
const excludeLSTs = new Set(options.exclude)

const checkMinBlock = (b) => {
  if (blockTag < b) {
    console.error(`${blockTag} is too early (minimum block at least ${b})`)
    process.exit(1)
  }
  return b
}

const multicallLimit = parseInt(options.multicallLimit)
if (1 < multicallLimit) checkMinBlock(7929876)

const LSTs = new Map()
async function addLST(lstSymbol, f) {
  if (excludeLSTs.has(lstSymbol)) return
  LSTs.set(lstSymbol, await f())
}
const ERC20ABI = [
  'function balanceOf(address) view returns (uint256)',
  'event Transfer(address indexed sender, address indexed receiver, uint256 value)'
]
await addLST('stETH', () => ({
  c: new ethers.Contract('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84', ERC20ABI, provider),
  b: checkMinBlock(11473216),
  r: oneEther
}))
await addLST('wstETH', async () => {
  const c = new ethers.Contract('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0',
    ERC20ABI.concat(['function getStETHByWstETH(uint256) view returns (uint256)']), provider)
  const b = checkMinBlock(11888477)
  const r = await c.getStETHByWstETH(oneEther, {blockTag})
  return {c, b, r}
})
await addLST('rETH', async () => {
  const c = new ethers.Contract('0xae78736Cd615f374D3085123A210448E74Fc6393',
    ERC20ABI.concat(['function getExchangeRate() view returns (uint256)']), provider)
  const b = checkMinBlock(13325304)
  const r = await c.getExchangeRate({blockTag})
  return {c, b, r}
})
await addLST('cbETH', async () => {
  const c = new ethers.Contract('0xbe9895146f7af43049ca1c1ae358b0541ea49704',
    ERC20ABI.concat(['function exchangeRate() view returns (uint256)']), provider)
  const b = checkMinBlock(14133762)
  const r = await c.exchangeRate({blockTag})
  return {c, b, r}
})
await addLST('frxETH', () => ({
  c: new ethers.Contract('0x5E8422345238F34275888049021821E8E08CAa1f', ERC20ABI, provider),
  b: checkMinBlock(15686046),
  r: oneEther
}))
await addLST('sfrxETH', async () => {
  const c = new ethers.Contract('0xac3E018457B222d93114458476f3E3416Abbe38F',
    ERC20ABI.concat(['function pricePerShare() view returns (uint256)']), provider)
  const b = checkMinBlock(15686046)
  const r = await c.pricePerShare({blockTag})
  return {c, r}
})
await addLST('swETH', async () => {
  const c = new ethers.Contract('0xf951E335afb289353dc249e82926178EaC7DEd78',
    ERC20ABI.concat(['function swETHToETHRate() view returns (uint256)']), provider)
  const b = checkMinBlock(17030767)
  const r = await c.swETHToETHRate({blockTag})
  return {c, b, r}
})
await addLST('wBETH', async () => {
  const c = new ethers.Contract('0xa2E3356610840701BDf5611a53974510Ae27E2e1',
    ERC20ABI.concat(['function exchangeRate() view returns (uint256)']), provider)
  const b = checkMinBlock(17080241)
  const r = await c.exchangeRate({blockTag})
  return {c, b, r}
})
// TODO: add diva when launched

const needsLastBlock = new Set(LSTs.keys())
const lastBlocks = new Map()
for (const key of db.getKeys({reverse: true})) {
  const [block, lstSymbol] = key.split('/')
  if (needsLastBlock.has(lstSymbol)) {
    needsLastBlock.delete(lstSymbol)
    lastBlocks.set(lstSymbol, block)
  }
  if (!needsLastBlock.size) break
}

const maxQueryRange = parseInt(options.maxQueryRange)
if (maxQueryRange > 10000) {
  console.error(`max query range too large (> 10000)`)
  process.exit(1)
}
if (multicallLimit > 10000) {
  console.error(`multicall limit too large (> 10000)`)
  process.exit(1)
}

async function getHolders(lstSymbol, lstContract, deployBlock) {
  const key = `${blockTag}/${lstSymbol}`
  const cached = db.get(key)
  if (typeof cached != 'undefined') return cached
  const lastBlock = lastBlocks.get(lstSymbol)
  const lastHolders = lastBlock ?
    new Set(db.get(`${lastBlock}/${lstSymbol}`).keys()) :
    new Set()
  let startBlock = parseInt(lastBlock || deployBlock)
  while (startBlock <= blockTag) {
    const endBlock = Math.min(blockTag, startBlock + maxQueryRange - 1)
    console.log(`${timestamp()} ${lstSymbol} ${startBlock}..${endBlock}`)
    const logs = await lstContract.queryFilter('Transfer', startBlock, endBlock)
    for (const log of logs) lastHolders.add(log.args.receiver)
    startBlock = endBlock + 1
  }
  const holders = new Map()
  const lastHoldersSize = lastHolders.size.toString()
  let numProcessed = 0
  if (multicallLimit <= 1) {
    for (const holder of lastHolders.values()) {
      console.log(`${timestamp()} ${lstSymbol} getting ${holder} balance @ ${blockTag} (${
        (++numProcessed).toString().padStart(lastHoldersSize.length, '0')}/${lastHoldersSize})`)
      const balance = await lstContract.balanceOf(holder, {blockTag})
      if (balance) holders.set(holder, balance.toString())
    }
  }
  else {
    const fragment = lstContract.interface.getFunction('balanceOf(address)')
    const lstAddress = await lstContract.getAddress()
    const getCallData = (holder) => lstContract.interface.encodeFunctionData(fragment, [holder])
    const holdersToProcess = Array.from(lastHolders.values())
    while (holdersToProcess.length) {
      const batch = holdersToProcess.splice(0, multicallLimit)
      const calls = batch.map(holder => [lstAddress, getCallData(holder)])
      console.log(`${timestamp()} ${lstSymbol} getting ${calls.length} balances @ ${blockTag} (${
        numProcessed.toString().padStart(lastHoldersSize.length, '0')}/${lastHoldersSize})`)
      const [blockNum, results] = await multicall.aggregate(calls, {blockTag})
      numProcessed += calls.length
      if (parseInt(blockNum) != blockTag && calls.length == results.length) {
        console.error(`Unexpected block ${blockNum} or length ${results.length} from multicall (wanted ${blockTag}, ${calls.length})`)
        process.exit(1)
      }
      for (const [i, holder] of batch.entries()) {
        const balance = lstContract.interface.decodeFunctionResult(fragment, results[i])
        if (balance) holders.set(holder, balance.toString())
      }
    }
  }
  await db.put(key, holders)
  return holders
}

function add(map, key, increment) {
  if (map.has(key))
    map.set(key, map.get(key) + increment)
  else
    map.set(key, increment)
}

const ETHvalue = new Map()
const lstHolders = new Map()

for (const [lstSymbol, {c: lstContract, b: deployBlock, r: rate}] of LSTs.entries()) {
  console.log(`${timestamp()} Getting holders for ${lstSymbol}`)
  const holders = await getHolders(lstSymbol, lstContract, deployBlock)
  lstHolders.set(lstSymbol, holders)
  for (const [holder, balance] of holders.entries())
    add(ETHvalue, holder, BigInt(balance) * rate / oneEther)
}

console.log(`${timestamp()} Sorting holders by ETH value`)
const sorted = Array.from(ETHvalue.entries())
  .toSorted(([h1, v1], [h2, v2]) => v1 < v2 ? 1 : v1 > v2 ? -1 : 0)
  .slice(0, parseInt(options.numTopHolders))

const filename = options.filename || `lstdiv-${blockTag}-${Array.from(LSTs.keys()).join('+')}-${options.numTopHolders}.json`
const outputFile = createWriteStream(filename)
const writeOut = async s => new Promise(resolve =>
  outputFile.write(s) ? resolve() : outputFile.once('drain', resolve))
const endOut = (s) => new Promise(resolve => outputFile.end(s, resolve))

const makeDelim = (fst) => {
  let f
  f = () => {
    f = () => ','
    return fst
  }
  return () => f()
}

const labels = JSON.parse(readFileSync('etherscan-labels/data/etherscan/combined/combinedAccountLabels.json'))

console.log(`${timestamp()} Writing to ${filename}`)
const blockTime = await provider.getBlock(blockTag).then(b => b.timestamp)
await writeOut(`{"blockNumber":${blockTag},"timestamp":${blockTime},"data":`)
const d0 = makeDelim('{')

for (const lstSymbol of LSTs.keys()) {
  await writeOut(`${d0()}"${lstSymbol}":`)
  const holders = lstHolders.get(lstSymbol)
  const d1 = makeDelim('[')
  for (const [holder] of sorted) {
    const amount = holders.get(holder)
    if (!amount) continue
    await writeOut(`${d1()}{"address":"${holder}",`)
    const label = labels[holder.toLowerCase()]
    if (label && label.name) await writeOut(`"entity":"${label.name}",`)
    await writeOut(`"amount":"${amount}"}`)
  }
  await writeOut(']')
}
await endOut('}}')
