import React from 'react';
import {ReactFlowProvider} from "@xyflow/react";

type Props = {
  children: React.ReactNode;
};
import '@xyflow/react/dist/style.css';
const Layout = ({children}: Props) => {
  return (
      <div className="h-screen overflow-scroll rounded-l-3xl border-l-[1px] border-t-[1px] border-muted-foreground/20 pb-20">
        <ReactFlowProvider>{children}</ReactFlowProvider>
      </div>
  );
};

export default Layout;