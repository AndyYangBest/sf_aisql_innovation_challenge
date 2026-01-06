import { useState } from "react";
import { FileText, Save, Edit3, Eye } from "lucide-react";
import { useTableStore } from "@/store/tableStore";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/hooks/use-toast";

interface NotesTabProps {
  tableId: string;
}

const NotesTab = ({ tableId }: NotesTabProps) => {
  const { getArtifactsByTable, addArtifact } = useTableStore();
  const artifacts = getArtifactsByTable(tableId);
  const docArtifacts = artifacts.filter((a) => a.type === "doc");
  const latestDoc = docArtifacts[docArtifacts.length - 1];

  const [isEditing, setIsEditing] = useState(false);
  const [content, setContent] = useState(latestDoc?.type === "doc" ? latestDoc.content.markdown : "");
  const { toast } = useToast();

  const handleSave = () => {
    if (content.trim()) {
      addArtifact({
        type: "doc",
        id: `doc-${Date.now()}`,
        tableId,
        content: { markdown: content },
        createdAt: new Date().toISOString(),
      });
      toast({ title: "Notes saved" });
      setIsEditing(false);
    }
  };

  // Simple markdown renderer
  const renderMarkdown = (md: string) => {
    return md.split("\n").map((line, i) => {
      if (line.startsWith("# ")) {
        return (
          <h1 key={i} className="text-2xl font-bold mb-4 mt-6 first:mt-0">
            {line.slice(2)}
          </h1>
        );
      }
      if (line.startsWith("## ")) {
        return (
          <h2 key={i} className="text-xl font-semibold mb-3 mt-5">
            {line.slice(3)}
          </h2>
        );
      }
      if (line.startsWith("### ")) {
        return (
          <h3 key={i} className="text-lg font-medium mb-2 mt-4">
            {line.slice(4)}
          </h3>
        );
      }
      if (line.startsWith("- ")) {
        return (
          <li key={i} className="ml-4 mb-1 text-muted-foreground">
            {formatInlineStyles(line.slice(2))}
          </li>
        );
      }
      if (line.match(/^\d+\. /)) {
        return (
          <li key={i} className="ml-4 mb-1 text-muted-foreground list-decimal">
            {formatInlineStyles(line.replace(/^\d+\. /, ""))}
          </li>
        );
      }
      if (line.startsWith("---")) {
        return <hr key={i} className="my-4 border-border" />;
      }
      if (line.startsWith("*") && line.endsWith("*")) {
        return (
          <p key={i} className="text-sm text-muted-foreground italic mb-2">
            {line.slice(1, -1)}
          </p>
        );
      }
      if (line.trim() === "") {
        return <div key={i} className="h-2" />;
      }
      return (
        <p key={i} className="mb-2 text-foreground/90">
          {formatInlineStyles(line)}
        </p>
      );
    });
  };

  const formatInlineStyles = (text: string) => {
    // Handle **bold**
    const parts = text.split(/(\*\*[^*]+\*\*)/g);
    return parts.map((part, i) => {
      if (part.startsWith("**") && part.endsWith("**")) {
        return (
          <strong key={i} className="font-semibold text-foreground">
            {part.slice(2, -2)}
          </strong>
        );
      }
      return part;
    });
  };

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold mb-1">Notes</h2>
          <p className="text-sm text-muted-foreground">
            Documentation and generated content
          </p>
        </div>
        <div className="flex items-center gap-2">
          {isEditing ? (
            <>
              <Button variant="outline" size="sm" onClick={() => setIsEditing(false)}>
                <Eye className="h-4 w-4 mr-2" />
                Preview
              </Button>
              <Button size="sm" onClick={handleSave}>
                <Save className="h-4 w-4 mr-2" />
                Save
              </Button>
            </>
          ) : (
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setContent(latestDoc?.type === "doc" ? latestDoc.content.markdown : "");
                setIsEditing(true);
              }}
            >
              <Edit3 className="h-4 w-4 mr-2" />
              Edit
            </Button>
          )}
        </div>
      </div>

      {isEditing ? (
        <Textarea
          value={content}
          onChange={(e) => setContent(e.target.value)}
          className="min-h-[500px] font-mono text-sm bg-card"
          placeholder="Write your notes in Markdown..."
        />
      ) : latestDoc && latestDoc.type === "doc" ? (
        <div className="p-6 rounded-xl glass prose prose-invert max-w-none">
          {renderMarkdown(latestDoc.content.markdown)}
        </div>
      ) : (
        <div className="flex items-center justify-center h-64 text-muted-foreground">
          <div className="text-center">
            <FileText className="h-12 w-12 mx-auto mb-3 opacity-50" />
            <p className="mb-2">No notes yet</p>
            <p className="text-sm mb-4">
              Use "Generate Doc" in AI Actions or create your own
            </p>
            <Button variant="outline" onClick={() => setIsEditing(true)}>
              <Edit3 className="h-4 w-4 mr-2" />
              Start Writing
            </Button>
          </div>
        </div>
      )}
    </div>
  );
};

export default NotesTab;
