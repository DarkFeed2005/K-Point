import React, { useState, useEffect } from 'react';
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { Power, Link2, Copy, Terminal, CheckCircle2, XCircle, Globe } from 'lucide-react';

interface LogEvent { level: string; message: string; timestamp: number; }
interface StatusEvent { status: string; public_url: string | null; }

const App = () => {
  const [isConnected, setIsConnected] = useState(false);
  const [localPort, setLocalPort] = useState('8080');
  const [serverHost, setServerHost] = useState('your-vps-ip'); // Change to your VPS
  const [subdomain, setSubdomain] = useState('my-project');
  const [publicUrl, setPublicUrl] = useState('');
  const [logs, setLogs] = useState<string[]>(['Ready to initialize K-Point.']);

  // Listen for events from Rust
  useEffect(() => {
    const unlistenLog = listen<LogEvent>('log', (event) => {
      setLogs((prev) => [`> ${event.payload.message}`, ...prev].slice(0, 10));
    });
    const unlistenStatus = listen<StatusEvent>('status', (event) => {
      setIsConnected(event.payload.status === 'Online');
      setPublicUrl(event.payload.public_url || '');
    });

    return () => {
      unlistenLog.then(f => f());
      unlistenStatus.then(f => f());
    };
  }, []);

  const handleToggleTunnel = async () => {
    try {
      if (isConnected) {
        await invoke('stop_tunnel');
      } else {
        await invoke('start_tunnel', { 
          localPort: parseInt(localPort), 
          serverHost, 
          subdomain 
        });
      }
    } catch (err) {
      setLogs((prev) => [`> Error: ${err}`, ...prev]);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 p-4 font-sans flex flex-col items-center justify-center">
      <div className="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl overflow-hidden">
        
        {/* Header */}
        <div className="bg-slate-950/50 p-6 text-center border-b border-slate-800">
          <h1 className="text-3xl font-black tracking-tighter text-orange-500">K-POINT</h1>
          <p className="text-slate-500 text-xs tracking-widest uppercase">By KpolitX</p>
          <div className={`mt-3 inline-flex items-center px-3 py-1 rounded-full text-xs border ${
            isConnected ? 'bg-green-950/30 text-green-400 border-green-900' : 'bg-slate-800 text-slate-400 border-slate-700'
          }`}>
            {isConnected ? 'LIVE' : 'OFFLINE'}
          </div>
        </div>

        {/* Inputs */}
        <div className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-xs font-bold text-slate-500 uppercase">Local Port</label>
              <input type="number" value={localPort} onChange={(e)=>setLocalPort(e.target.value)} disabled={isConnected}
                className="w-full bg-slate-950 border border-slate-800 p-2 rounded text-orange-400 font-mono" />
            </div>
            <div>
              <label className="text-xs font-bold text-slate-500 uppercase">Subdomain</label>
              <input type="text" value={subdomain} onChange={(e)=>setSubdomain(e.target.value)} disabled={isConnected}
                className="w-full bg-slate-950 border border-slate-800 p-2 rounded text-orange-400 font-mono" />
            </div>
          </div>
          
          <div>
            <label className="text-xs font-bold text-slate-500 uppercase">Relay Server (VPS IP)</label>
            <input type="text" value={serverHost} onChange={(e)=>setServerHost(e.target.value)} disabled={isConnected}
              className="w-full bg-slate-950 border border-slate-800 p-2 rounded text-slate-300 font-mono" />
          </div>

          <button onClick={handleToggleTunnel}
            className={`w-full py-3 rounded-lg font-bold flex items-center justify-center transition-all ${
              isConnected ? 'bg-red-900/50 hover:bg-red-800 text-white' : 'bg-orange-600 hover:bg-orange-500 text-white shadow-lg shadow-orange-900/20'
            }`}>
            <Power className="w-5 h-5 mr-2" /> {isConnected ? 'STOP TUNNEL' : 'START K-POINT'}
          </button>
        </div>

        {/* Connection Link */}
        {isConnected && (
          <div className="px-6 pb-6 animate-in fade-in slide-in-from-bottom-2">
            <div className="bg-slate-950 border border-orange-500/30 p-3 rounded-lg flex items-center justify-between">
              <code className="text-green-400 text-sm truncate">{publicUrl}</code>
              <button onClick={() => navigator.clipboard.writeText(publicUrl)} className="text-slate-500 hover:text-white"><Copy size={16}/></button>
            </div>
          </div>
        )}

        {/* Logs */}
        <div className="bg-black/40 p-4 border-t border-slate-800 h-32 overflow-y-auto font-mono text-[10px] text-slate-600">
          {logs.map((l, i) => <div key={i}>{l}</div>)}
        </div>
      </div>
    </div>
  );
};

export default App;