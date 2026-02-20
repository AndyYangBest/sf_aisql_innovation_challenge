import { useState, useEffect } from "react";
import { Settings, Save, X, Database, AlertCircle, LogIn, Loader2 } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { tablesApi } from "@/api/tables";
import {
  type SnowflakeConfig,
  DEFAULT_SNOWFLAKE_CONFIG,
  getSnowflakeConfig,
  saveSnowflakeConfig,
  clearSnowflakeConfig,
  hasUserConfig,
} from "@/lib/snowflakeConfig";

interface SnowflakeConfigDialogProps {
  trigger?: React.ReactNode;
}

const SnowflakeConfigDialog = ({ trigger }: SnowflakeConfigDialogProps) => {
  const [open, setOpen] = useState(false);
  const [config, setConfig] = useState<SnowflakeConfig>(getSnowflakeConfig());
  const [isUserConfig, setIsUserConfig] = useState(hasUserConfig());
  const [isAuthorizingSSO, setIsAuthorizingSSO] = useState(false);
  const { toast } = useToast();

  useEffect(() => {
    if (open) {
      setConfig(getSnowflakeConfig());
      setIsUserConfig(hasUserConfig());
    }
  }, [open]);

  const handleSave = () => {
    const usingExternalBrowser = config.authenticator?.trim().toLowerCase() === "externalbrowser";
    // 验证必填字段
    if (!config.account || !config.user || (!usingExternalBrowser && !config.password)) {
      toast({
        title: "Validation Error",
        description: usingExternalBrowser
          ? "Account and User are required for browser SSO"
          : "Account, User, and Password are required",
        variant: "destructive",
      });
      return;
    }

    saveSnowflakeConfig(config);
    setIsUserConfig(true);
    toast({
      title: "Configuration Saved",
      description: "Your Snowflake credentials have been saved locally",
    });
    setOpen(false);
  };

  const handleAuthorizeSSO = async () => {
    const account = config.account.trim();
    const user = config.user.trim();

    if (!account || !user) {
      toast({
        title: "Validation Error",
        description: "Account and User are required for browser SSO",
        variant: "destructive",
      });
      return;
    }

    const nextConfig: SnowflakeConfig = {
      ...config,
      account,
      user,
      password: "",
      authenticator: "externalbrowser",
    };

    saveSnowflakeConfig(nextConfig);
    setConfig(nextConfig);
    setIsUserConfig(true);
    setIsAuthorizingSSO(true);

    try {
      const response = await tablesApi.getSnowflakeDatabases();
      if (response.status === "success") {
        toast({
          title: "SSO Authorized",
          description: "Browser authorization completed",
        });
        setOpen(false);
        return;
      }

      toast({
        title: "SSO Authorization Failed",
        description: response.error || "Please complete browser login and retry",
        variant: "destructive",
      });
    } catch {
      toast({
        title: "SSO Authorization Failed",
        description: "Could not reach backend for browser authorization",
        variant: "destructive",
      });
    } finally {
      setIsAuthorizingSSO(false);
    }
  };

  const handleReset = () => {
    clearSnowflakeConfig();
    setConfig(DEFAULT_SNOWFLAKE_CONFIG);
    setIsUserConfig(false);
    toast({
      title: "Configuration Reset",
      description: "Using default Snowflake credentials",
    });
  };

  const handleChange = (field: keyof SnowflakeConfig, value: string) => {
    setConfig((prev) => ({ ...prev, [field]: value }));
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        {trigger || (
          <Button variant="outline" size="sm" className="gap-2">
            <Settings className="h-4 w-4" />
            <span className="hidden sm:inline">Config</span>
          </Button>
        )}
      </DialogTrigger>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Snowflake Configuration
          </DialogTitle>
          <DialogDescription>
            Configure account/user/password, or use one-click browser SSO authorization.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {!isUserConfig && (
            <Alert>
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>
                Currently using backend default Snowflake credentials
              </AlertDescription>
            </Alert>
          )}

          <div className="space-y-2">
            <Label htmlFor="account">Account *</Label>
            <Input
              id="account"
              value={config.account}
              onChange={(e) => handleChange("account", e.target.value)}
              placeholder="WKUKTVG-CX42955"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="user">User *</Label>
            <Input
              id="user"
              value={config.user}
              onChange={(e) => handleChange("user", e.target.value)}
              placeholder="your_username"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="password">Password (required unless externalbrowser)</Label>
            <Input
              id="password"
              type="password"
              value={config.password}
              onChange={(e) => handleChange("password", e.target.value)}
              placeholder="your_password (not required for externalbrowser)"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="authenticator">Authenticator (optional)</Label>
            <Input
              id="authenticator"
              value={config.authenticator || ""}
              onChange={(e) => handleChange("authenticator", e.target.value)}
              placeholder="externalbrowser"
            />
          </div>

          <Button
            type="button"
            variant="secondary"
            className="w-full gap-2"
            onClick={handleAuthorizeSSO}
            disabled={isAuthorizingSSO}
          >
            {isAuthorizingSSO ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                Authorizing SSO...
              </>
            ) : (
              <>
                <LogIn className="h-4 w-4" />
                Login with Snowflake SSO
              </>
            )}
          </Button>
        </div>

        <div className="flex justify-between gap-3">
          <Button variant="outline" onClick={handleReset}>
            <X className="h-4 w-4 mr-2" />
            Reset to Default
          </Button>
          <Button onClick={handleSave}>
            <Save className="h-4 w-4 mr-2" />
            Save Configuration
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default SnowflakeConfigDialog;
