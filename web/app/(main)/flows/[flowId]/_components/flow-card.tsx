"use client"
import React, {useEffect, useState} from 'react';
import {NodeResizer, Position, useReactFlow} from "@xyflow/react";
import {Card, CardContent, CardDescription, CardHeader, CardTitle} from "@/components/ui/card";
import {FlowCardType, FlowNodeType} from "@/types/editor";
import {Button} from "@/components/ui/button";
import CustomHandle from "@/app/(main)/flows/[flowId]/_components/custom-handle";
import FlowCardInfobox from "@/app/(main)/flows/[flowId]/_components/flow-card-infobox";
import FocusInput from "@/app/(main)/flows/[flowId]/_components/focus-input";

type Props = {
  id: string;
  data: FlowCardType;
  selected?: boolean | undefined;
};
const FlowCard = ({id, data, selected}: Props) => {
  const [title, setTitle] = useState(data.title);
  const [isTitleEditing, setIsTitleEditing] = useState(false);
  const [description, setDescription] = useState(data.description);
  const [isDescriptionEditing, setIsDescriptionEditing] = useState(false);
  const [isEditing, setIsEditing] = useState(false);
  const {updateNodeData, setNodes} = useReactFlow<FlowNodeType>();
  const onNodeClick = () => {
    setNodes(nodes => {
      return nodes.map(n => {
        if (n.id === id) {
          return {
            ...n,
            selected: true,
          };
        }
        return {...n, selected: false};
      });
    });
  };
  const onSubmit = () => {
    updateNodeData(id, {
      title: title,
      description: description,
    });
    setIsEditing(false);
  };
  useEffect(() => {
    if (data.title !== title) {
      setIsEditing(true);
    }
    if (data.description !== description) {
      setIsEditing(true);
    }
    if (data.title === title && data.description === description) {
      setIsEditing(false);
    }
  }, [data.description, data.title, description, title]);
  return  (
      <>
        <NodeResizer
            color="#ff0071"
            isVisible={selected}
            minWidth={100}
            minHeight={30}
        />
        {data.type != 'Start' && (
            <CustomHandle
                type="target"
                position={Position.Top}
                style={{zIndex: 100}}
            />
        )}
        <Card
            onClick={e => {
              e.stopPropagation();
              onNodeClick();
            }}
            className="relative dark:border-muted-foreground/70 w-full h-full"
        >
          <FlowCardInfobox id={id} type={data.type} />
          <CardHeader className="flex flex-row items-center pt-0 gap-4">
            <div>
              {isTitleEditing ? (
                  <CardTitle className="text-md">
                    <FocusInput
                        value={title}
                        editStatus={isTitleEditing}
                        onChange={setTitle}
                        onBlur={() => setIsTitleEditing(false)}
                    />
                  </CardTitle>
              ) : (
                  <CardTitle
                      className="text-md cursor-pointer"
                      onClick={() => setIsTitleEditing(true)}
                  >
                    {title}
                  </CardTitle>
              )}
              {isDescriptionEditing ? (
                  <CardDescription>
                    <FocusInput
                        value={description}
                        editStatus={isDescriptionEditing}
                        onChange={setDescription}
                        onBlur={() => setIsDescriptionEditing(false)}
                    />
                  </CardDescription>
              ) : (
                  <CardDescription
                      className={'cursor-pointer'}
                      onClick={() => setIsDescriptionEditing(true)}
                  >
                    <p>{description}</p>
                  </CardDescription>
              )}
            </div>
          </CardHeader>
          <CardContent className={'flex flex-row justify-between'}>
            {isEditing && (
                <div className={'flex flex-col justify-end'}>
                  <Button onClick={onSubmit}>Submit</Button>
                </div>
            )}
          </CardContent>
        </Card>
        {data.type !== 'End' && (
            <CustomHandle type="source" position={Position.Bottom} id="a" />
        )}
      </>
  );
};

export default FlowCard;