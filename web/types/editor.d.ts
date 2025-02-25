export const FLowStatus = ['completed', 'current', 'pending'] as const;
export type FlowTypes = 'Start' | 'Condition' | 'Trigger' | 'Action' | 'End';
export type FlowStatusTypes = (typeof FLowStatus)[number];
export type FlowCardType = {
  title: string;
  description: string;
  completed: boolean;
  current: boolean;
  metadata: object;
  type: FlowTypes;
  status: FlowStatusTypes;
  fileUrl: string | null;
  fileName: string | null;
};

export type FlowNodeType = {
  id: string;
  type: FlowCardType['type'];
  selected: boolean;
  position: {
    x: number;
    y: number;
  };
  data: FlowCardType;
};