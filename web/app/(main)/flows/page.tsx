import React from 'react';
import Link from "next/link";
import {getFlows} from "@/app/(main)/flows/_actions/flow-action";

const Page = async () => {
  const flows = await getFlows()
  console.log(flows)
  return (
      <div>
        <div>
          {flows.map((flow) => (
              <div key={flow.id}>
                <Link href={`/flows/${flow.id}`}>{flow.name}</Link>
              </div>
          ))}
        </div>
      </div>
  );
};

export default Page;