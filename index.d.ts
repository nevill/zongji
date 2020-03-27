import {EventEmitter} from 'events'
import {Connection, ConnectionConfig, Pool} from 'mysql'

declare class ZongJi extends EventEmitter {
  public tableMap: {[tableId: number]: ZongJi.ITableMap}
  public ready: boolean
  public useChecksum:boolean
  public options: ZongJi.IStartOptions
  public connection: Connection

  constructor(options: ConnectionConfig | Pool | Connection)

  public start(options: ZongJi.IStartOptions): void
  public stop(): void

  public on(event: 'binlog', listener: (event: ZongJi.IBinlogEventData) => unknown): this
  public on(event: 'ready', listener: () => unknown): this
  public on(event: 'error', listener: (err: Error) => unknown): this
  public on(event: 'stopped', listener: () => unknown): this

  public once(event: 'binlog', listener: (event: ZongJi.IBinlogEventData) => unknown): this
  public once(event: 'ready', listener: () => unknown): this
  public once(event: 'error', listener: (err: Error) => unknown): this
  public once(event: 'stopped', listener: () => unknown): this

  public get(name: string): unknown
  public get(name: string[]): unknown[]
}

declare namespace ZongJi {
  export type TEvents = 'unknown' | 'query' | 'intvar' | 'rotate' | 'format' | 'xid' | 'tablemap' | 'writerows' |
    'updaterows' | 'deleterows'

  type BIGINT = string | number

  export interface IStartOptions {
    serverId: number
    startAtEnd?: boolean
    filename?: string
    position?: number
    nonBlock?: boolean
    includeEvents?: TEvents[]
    excludeEvents?: TEvents[]
    includeSchema?: {[schema: string]: boolean | string[]}
    excludeSchema?: {[schema: string]: boolean | string[]}
  }

  export interface IBinlogEventData {
    timestamp: number
    nextPosition: number
    size: number
    new(parser, options?)
    getEventName(): TEvents
    getTypeName(): string
    dump(): void
  }

  export interface IRowsEventData extends IBinlogEventData {
    rows: {before: object; after: object}[]
    tableId: number
    tableMap: {[tableId: number]: ITableMap}
    flags: number
    useChecksum: boolean
    extraDataLength: number
    numberOfColumns: number
    setTableMap(tableMap: {[tableId: number]: ITableMap}): void
  }

  export interface IRotateEventData extends IBinlogEventData {
    position: BIGINT
    binlogName: string
  }

  export interface IFormatEventData extends IBinlogEventData {
    [key: string]: unknown
  }

  export interface IXidEventData extends IBinlogEventData {
    xid: BIGINT
  }

  export interface IQueryEventData extends IBinlogEventData {
    slaveProxyId: number
    executionTime: number
    schemaLength: number
    errCode: number
    statusVarsLength: number
    statusVars: string
    schema: string
    query: string
  }

  export interface IIntVarEventData extends IBinlogEventData {
    type: 1 | 2
    value: BIGINT
    getIntTypeName(): 'INVALID_INT' | 'LAST_INSERT_ID' | 'INSERT_ID'
  }

  export interface ITableMapEventData extends IBinlogEventData {
    tableMap: {[tableId: number]: ITableMap}
    flags: number
    schemaName: string
    tableName: string
    columnCount: number
    columnType: number[]
    updateColumnInfo(): void
  }

  export interface ITableMap {
    parentSchema: string
    tableName: string
  }
}

export = ZongJi
