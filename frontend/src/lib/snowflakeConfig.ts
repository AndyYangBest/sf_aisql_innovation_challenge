export interface SnowflakeConfig {
  account: string;
  user: string;
  password: string;
  authenticator?: string;
  warehouse?: string;
  database?: string;
  schema?: string;
  role?: string;
}

export const SNOWFLAKE_CONFIG_STORAGE_KEY = "snowflake_user_config";
export const SNOWFLAKE_CONFIG_HEADER = "X-Snowflake-Config";
export const SNOWFLAKE_CONFIG_UPDATED_EVENT = "snowflake-config-updated";

export const DEFAULT_SNOWFLAKE_CONFIG: SnowflakeConfig = {
  account: "",
  user: "",
  password: "",
  authenticator: "",
  warehouse: "",
  database: "",
  schema: "",
  role: "",
};

const canUseBrowserStorage = () => typeof window !== "undefined" && typeof localStorage !== "undefined";

const encodeConfig = (config: Record<string, string>): string | null => {
  if (typeof globalThis.btoa !== "function") {
    return null;
  }
  try {
    return globalThis.btoa(JSON.stringify(config));
  } catch {
    return null;
  }
};

const emitConfigChanged = () => {
  if (typeof window === "undefined") {
    return;
  }
  window.dispatchEvent(new CustomEvent(SNOWFLAKE_CONFIG_UPDATED_EVENT));
};

export const getSnowflakeConfig = (): SnowflakeConfig => {
  if (!canUseBrowserStorage()) {
    return DEFAULT_SNOWFLAKE_CONFIG;
  }
  try {
    const stored = localStorage.getItem(SNOWFLAKE_CONFIG_STORAGE_KEY);
    if (!stored) {
      return DEFAULT_SNOWFLAKE_CONFIG;
    }
    const parsed = JSON.parse(stored) as Partial<SnowflakeConfig>;
    const usingExternalBrowser = parsed.authenticator?.trim().toLowerCase() === "externalbrowser";
    if (!parsed.account || !parsed.user || (!usingExternalBrowser && !parsed.password)) {
      return DEFAULT_SNOWFLAKE_CONFIG;
    }
    return {
      ...DEFAULT_SNOWFLAKE_CONFIG,
      ...parsed,
    };
  } catch {
    return DEFAULT_SNOWFLAKE_CONFIG;
  }
};

export const saveSnowflakeConfig = (config: SnowflakeConfig): void => {
  if (!canUseBrowserStorage()) {
    return;
  }
  localStorage.setItem(SNOWFLAKE_CONFIG_STORAGE_KEY, JSON.stringify(config));
  emitConfigChanged();
};

export const clearSnowflakeConfig = (): void => {
  if (!canUseBrowserStorage()) {
    return;
  }
  localStorage.removeItem(SNOWFLAKE_CONFIG_STORAGE_KEY);
  emitConfigChanged();
};

export const hasUserConfig = (): boolean => {
  if (!canUseBrowserStorage()) {
    return false;
  }
  return !!localStorage.getItem(SNOWFLAKE_CONFIG_STORAGE_KEY);
};

const buildHeaderPayload = (config: SnowflakeConfig): Record<string, string> => {
  const payload: Record<string, string> = {};

  const assignIfPresent = (key: string, value: string | undefined) => {
    if (typeof value !== "string") {
      return;
    }
    const normalized = value.trim();
    if (!normalized) {
      return;
    }
    payload[key] = normalized;
  };

  assignIfPresent("account", config.account);
  assignIfPresent("user", config.user);
  assignIfPresent("authenticator", config.authenticator);
  assignIfPresent("role", config.role);

  const usingExternalBrowser = (payload.authenticator || "").toLowerCase() === "externalbrowser";
  if (!usingExternalBrowser) {
    assignIfPresent("password", config.password);
  }

  return payload;
};

export const getEncodedSnowflakeConfig = (): string | null => {
  const payload = buildHeaderPayload(getSnowflakeConfig());
  if (!payload.account || !payload.user || (!payload.password && payload.authenticator?.toLowerCase() !== "externalbrowser")) {
    return null;
  }
  return encodeConfig(payload);
};

export const getSnowflakeConfigHeaders = (): Record<string, string> => {
  if (!hasUserConfig()) {
    return {};
  }
  const encoded = getEncodedSnowflakeConfig();
  if (!encoded) {
    return {};
  }
  return {
    [SNOWFLAKE_CONFIG_HEADER]: encoded,
  };
};
