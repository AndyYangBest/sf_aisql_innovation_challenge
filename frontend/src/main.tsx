import "reflect-metadata";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "@flowgram.ai/free-layout-editor/index.css";
import "./index.css";

createRoot(document.getElementById("root")!).render(<App />);
