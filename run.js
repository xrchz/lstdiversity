import { ethers } from 'ethers'
import { program } from 'commander'
import { createWriteStream } from 'node:fs'
import { open } from 'lmdb'

const options = program
  .option('-r, --rpc <url>', 'Full node RPC endpoint URL', 'http://localhost:8545')
  .option('-b, --block <num>', 'Block number to collect data for (default: latest)')
  .option('--prune', 'Prune the database of blocks before <block> then exit')
  .option('-m, --max-query-range <num>', 'Maximum number of blocks to query for events at a time', 1000)
  .option('-n, --num-top-holders <num>', 'Number of top holders (by total ETH value) to include in output file', 100)
  .option('-f, --filename <name>', 'Name of output file (default: lstdiv-<block>.json)')
  .option('-x, --exclude <tokens...>', 'Symbols of LSTs to exclude')
  .parse()
  .opts()

const provider = new ethers.JsonRpcProvider(options.rpc)

const db = open({path: 'db'})
// <block>/<lstSymbol> : map from address to balance of holders

const timestamp = () => Intl.DateTimeFormat('en-GB',
  {hour: 'numeric', minute: 'numeric', second: 'numeric'})
  .format(new Date())

const blockTag = options.block ? parseInt(options.block) : await provider.getBlockNumber()
console.log(`${timestamp()} Working @ block ${blockTag}`)

if (options.prune) {
  for (const key of db.getKeys()) {
    const keyBlock = parseInt(key.split('/')[0])
    if (keyBlock < blockTag)
      await db.remove(key)
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

async function getHolders(lstSymbol, lstContract, deployBlock) {
  const key = `${blockTag}/${lstSymbol}`
  const cached = db.get(key)
  if (typeof cached != 'undefined') return cached
  const lastBlock = lastBlocks.get(lstSymbol)
  const lastHolders = lastBlock ?
    new Set(db.get(`${lastBlock}/${lstSymbol}`).keys()) :
    new Set()
  let startBlock = parseInt(lastBlock || deployBlock)
  while (startBlock < blockTag) {
    const endBlock = Math.min(blockTag, startBlock + maxQueryRange - 1)
    console.log(`${timestamp()} ${lstSymbol} ${startBlock}..${endBlock}`)
    const logs = await lstContract.queryFilter('Transfer', startBlock, endBlock)
    for (const log of logs) lastHolders.add(log.args.receiver)
    startBlock = endBlock + 1
  }
  const holders = new Map()
  for (const holder of lastHolders.values()) {
    const balance = await lstContract.balanceOf(holder, {blockTag})
    if (balance) holders.set(holder, balance)
  }
  await db.set(key, holders)
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
    add(ETHvalue, holder, balance * rate / oneEther)
}

console.log(`${timestamp()} Sorting holders by ETH value`)
const sorted = Array.from(ETHvalue.entries())
  .toSorted(([h1, v1], [h2, v2]) => v1 - v2)
  .slice(0, parseInt(options.numTopHolders))

const outputFile = createWriteStream(options.filename)
const writeOut = async s => new Promise(resolve =>
  outputFile.write(s) ? resolve() : outputFile.once('drain', resolve))
const endOut = (s) => new Promise(resolve => outputFile.end(s, resolve))

const makeDelim = () => {
  let f
  f = () => {
    f = () => ','
    return '['
  }
  return () => f()
}

console.log(`${timestamp()} Writing to ${options.filename}`)
const blockTime = await provider.getBlock(blockTag).then(b => b.timestamp)
await writeOut(`{"blockNumber":${blockTag},"timestamp":${blockTime},"data":`)
const d0 = makeDelim()

for (const [holder] of sorted) {
  await writeOut(`${d0()}{"address":"${holder}","entity":"TODO","lsts":`)
  const d1 = makeDelim()
  for (const lstSymbol of LSTs.keys())
    await writeOut(`${d1()}{"lst":"${lstSymbol}","amount":"${lstHolders.get(lstSymbol).get(holder)}"}`)
  await writeOut(']')
}
await endOut(']}')
