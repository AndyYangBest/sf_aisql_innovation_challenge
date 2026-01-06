import { Link } from "react-router-dom";
import { Database, Home } from "lucide-react";
import { Button } from "@/components/ui/button";

const NotFound = () => {
  return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-center animate-fade-in">
        <div className="p-4 rounded-2xl bg-muted/30 inline-block mb-6">
          <Database className="h-16 w-16 text-muted-foreground" />
        </div>
        <h1 className="text-4xl font-bold mb-2">404</h1>
        <p className="text-xl text-muted-foreground mb-6">Page not found</p>
        <Button asChild>
          <Link to="/">
            <Home className="h-4 w-4 mr-2" />
            Back to Tables
          </Link>
        </Button>
      </div>
    </div>
  );
};

export default NotFound;