import React from 'react';
import {Card, CardDescription, CardHeader, CardTitle} from "@/components/ui/card";
import {EditorCanvasDefaultCardTypes} from "@/lib/constants";
import {FlowCardType, FlowTypes} from "@/types/editor";

const EditorSidebar = () => {

  const onDragStart = (event: React.DragEvent<HTMLDivElement>, nodeType: FlowCardType['type']) => {
    event.dataTransfer.setData('application/reactflow', nodeType);
    event.dataTransfer.effectAllowed = 'move';
  };
  return (
      <aside>
        <div className={'h-screen overflow-scroll pb-24'}>
          <div className={'flex flex-col gap-4 p-4'}>
            {Object.entries(EditorCanvasDefaultCardTypes).map(
                ([cardKey, cardValue]) => (
                    <Card
                        key={cardKey}
                        draggable
                        className="w-full cursor-grab border-black bg-neutral-100 dark:border-neutral-700 dark:bg-neutral-900"
                        onDragStart={event => onDragStart(event, cardKey as FlowTypes)}
                    >
                      <CardHeader className="flex flex-row items-center gap-4 p-4">
                        {/*<EditorCanvasIconHelper type={cardKey as EditorCanvasTypes} />*/}
                        <CardTitle className="text-md">
                          {cardKey}
                          <CardDescription>{cardValue.description}</CardDescription>
                        </CardTitle>
                      </CardHeader>
                    </Card>
                ),
            )}
          </div>
        </div>
      </aside>
  );
};

export default EditorSidebar;