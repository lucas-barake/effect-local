export const Avatar = ({ name, color, size = 40 }: {
  readonly name: string
  readonly color: string
  readonly size?: number
}) => (
  <span
    className="avatar"
    aria-hidden
    style={{ backgroundColor: color, width: size, height: size, fontSize: size * 0.42 }}
  >
    {name[0]}
  </span>
)
