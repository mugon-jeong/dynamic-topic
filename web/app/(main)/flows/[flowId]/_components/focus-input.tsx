import React, {useEffect, useRef} from 'react';
import {Input} from '@/components/ui/input';

type Props = {
  value: string;
  editStatus: boolean;
  onChange: (value: string) => void;
  onBlur: () => void;
};
const FocusInput = ({value, onChange, onBlur, editStatus}: Props) => {
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (editStatus && inputRef.current) {
      inputRef.current.focus();
    }
  }, [editStatus]);
  return (
      <Input
          ref={inputRef}
          value={value}
          onChange={event => onChange(event.target.value)}
          onBlur={onBlur}
      />
  );
};
export default FocusInput;