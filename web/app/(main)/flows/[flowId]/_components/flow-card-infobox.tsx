import React from 'react';
import {Badge} from '@/components/ui/badge';

type Props = {
  id: string;
  type: string;
};
const FlowCardInfobox = ({id, type}: Props) => {
  return (
      <div className={'flex flex-row justify-between items-center p-2 w-full'}>
        <p className="text-xs text-muted-foreground/50 pr-6">
          <b className="text-muted-foreground/80">ID: </b>
          {id}
        </p>
        <Badge variant="secondary" className="">
          {type}
        </Badge>
      </div>
  );
};
export default FlowCardInfobox;