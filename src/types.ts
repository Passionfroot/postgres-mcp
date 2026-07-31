export interface SourceConfig {
  id: string;
  dsn: string;
  readonly: boolean;
  maxRows: number;
  timeout: number;
  poolMax: number;
  allowMultiStatements: boolean;
  role?: string;
  sessionVars?: Record<string, string>;
  sshHost?: string;
  sshUser?: string;
  sshKey?: string;
}

export interface AuditLogConfig {
  logFile: string;
  maxSize?: number;
}

export interface Config {
  sources: SourceConfig[];
  prismaSchemaPath?: string;
  auditLog?: AuditLogConfig;
}

export interface TunnelConfig {
  sshHost: string;
  sshUser: string;
  sshKeyPath: string;
  remoteHost: string;
  remotePort: number;
  keepaliveInterval: number;
}

export interface TunnelHandle {
  localHost: string;
  localPort: number;
  close(): Promise<void>;
}
