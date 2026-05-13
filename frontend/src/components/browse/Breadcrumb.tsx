import { memo } from "react";
import { ChevronRight, Home } from "lucide-react";
import { goCatalogs, goSchemas, goLanding } from "../../hooks/useRouter";

interface Crumb {
  label: string;
  onClick?: () => void;
}

interface Props {
  catalog?: string;
  schema?: string;
}

function Breadcrumb({ catalog, schema }: Props) {
  const crumbs: Crumb[] = [{ label: "Home", onClick: goLanding }];
  crumbs.push({ label: "Catalogs", onClick: goCatalogs });
  if (catalog) crumbs.push({ label: catalog, onClick: schema ? () => goSchemas(catalog) : undefined });
  if (schema) crumbs.push({ label: schema });

  return (
    <nav className="flex items-center gap-1.5 text-[12px] font-mono" aria-label="Breadcrumb">
      {crumbs.map((c, i) => {
        const isLast = i === crumbs.length - 1;
        const Icon = i === 0 ? Home : null;
        return (
          <div key={i} className="flex items-center gap-1.5">
            {i > 0 && <ChevronRight size={12} className="text-slate-600" />}
            {c.onClick && !isLast ? (
              <button
                onClick={c.onClick}
                className="flex items-center gap-1 text-slate-400 hover:text-accent-light transition-colors"
              >
                {Icon && <Icon size={12} />}
                {!Icon && c.label}
              </button>
            ) : (
              <span className={`flex items-center gap-1 ${isLast ? "text-slate-200" : "text-slate-400"}`}>
                {Icon && <Icon size={12} />}
                {!Icon && c.label}
              </span>
            )}
          </div>
        );
      })}
    </nav>
  );
}

export default memo(Breadcrumb);
